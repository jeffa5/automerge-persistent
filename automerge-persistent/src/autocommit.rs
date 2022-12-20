use std::collections::HashMap;

use crate::{Error, PeerId, Persister};
use automerge::{sync, AutoCommit, Change, ChangeHash};

/// A wrapper for a persister and an automerge document.
#[derive(Debug)]
pub struct PersistentAutoCommit<P> {
    document: AutoCommit,
    sync_states: HashMap<PeerId, sync::State>,
    persister: P,
    saved_heads: Vec<ChangeHash>,
}

impl<P> PersistentAutoCommit<P>
where
    P: Persister + 'static,
{
    pub const fn document(&self) -> &AutoCommit {
        &self.document
    }

    /// UNSAFE: this may lead to changes not being immediately persisted
    pub fn document_mut(&mut self) -> &mut AutoCommit {
        &mut self.document
    }

    /// Make changes to the document but don't immediately persist changes.
    pub fn transact<F: FnOnce(&mut AutoCommit) -> Result<O, E>, O, E>(
        &mut self,
        f: F,
    ) -> Result<O, E> {
        let result = f(&mut self.document)?;
        // don't get the changes or anything as that will close the transaction, instead delay that
        // until another operation such as save or receive_sync_message etc.
        Ok(result)
    }

    /// Load the persisted changes (both individual changes and a document) from storage and
    /// rebuild the Backend.
    ///
    /// ```rust
    /// # use automerge_persistent::MemoryPersister;
    /// # use automerge_persistent::PersistentBackend;
    /// let persister = MemoryPersister::default();
    /// let backend = PersistentBackend::<_, automerge::Backend>::load(persister).unwrap();
    /// ```
    pub fn load(persister: P) -> Result<Self, Error<P::Error>> {
        let document = persister.get_document().map_err(Error::PersisterError)?;
        let mut backend = if let Some(document) = document {
            AutoCommit::load(&document).map_err(Error::AutomergeError)?
        } else {
            AutoCommit::new()
        };

        let change_bytes = persister.get_changes().map_err(Error::PersisterError)?;

        let mut changes = Vec::new();
        for change_bytes in change_bytes {
            changes.push(Change::from_bytes(change_bytes).map_err(Error::AutomergeLoadChangeError)?)
        }

        backend
            .apply_changes(changes)
            .map_err(Error::AutomergeError)?;

        let saved_heads = backend.get_heads();
        Ok(Self {
            document: backend,
            sync_states: HashMap::new(),
            persister,
            saved_heads,
        })
    }

    /// Compact the storage.
    ///
    /// This first obtains the changes currently in the backend, saves the backend and persists the
    /// saved document. We then can remove the previously obtained changes one by one.
    ///
    /// It also clears out the storage used up by old sync states for peers by removing those given
    /// in `old_peers`.
    ///
    /// ```rust
    /// # use automerge_persistent::MemoryPersister;
    /// # use automerge_persistent::PersistentBackend;
    /// # let persister = MemoryPersister::default();
    /// # let mut backend = PersistentBackend::<_, automerge::Backend>::load(persister).unwrap();
    /// backend.compact(&[]).unwrap();
    /// ```
    pub fn compact(&mut self, old_peer_ids: &[&[u8]]) -> Result<(), Error<P::Error>> {
        let saved_backend = self.document.save();
        self.saved_heads = self.document.get_heads();
        let changes = self.document.get_changes(&[])?;
        self.persister
            .set_document(saved_backend)
            .map_err(Error::PersisterError)?;
        self.persister
            .remove_changes(
                changes
                    .into_iter()
                    .map(|c| (c.actor_id(), c.seq()))
                    .collect(),
            )
            .map_err(Error::PersisterError)?;
        self.persister
            .remove_sync_states(old_peer_ids)
            .map_err(Error::PersisterError)?;
        Ok(())
    }

    /// Generate a sync message to be sent to a peer backend.
    ///
    /// Peer id is intentionally low level and up to the user as it can be a DNS name, IP address or
    /// something else.
    ///
    /// This internally retrieves the previous sync state from storage and saves the new one
    /// afterwards.
    ///
    /// ```rust
    /// # use automerge_persistent::MemoryPersister;
    /// # use automerge_persistent::PersistentBackend;
    /// # let persister = MemoryPersister::default();
    /// # let mut backend = PersistentBackend::<_, automerge::Backend>::load(persister).unwrap();
    /// let message = backend.generate_sync_message(vec![]).unwrap();
    /// ```
    pub fn generate_sync_message(
        &mut self,
        peer_id: PeerId,
    ) -> Result<Option<sync::Message>, Error<P::Error>> {
        self.close_transaction()?;

        if !self.sync_states.contains_key(&peer_id) {
            if let Some(sync_state) = self
                .persister
                .get_sync_state(&peer_id)
                .map_err(Error::PersisterError)?
            {
                let s = sync::State::decode(&sync_state).map_err(Error::AutomergeDecodeError)?;
                self.sync_states.insert(peer_id.clone(), s);
            }
        }
        let sync_state = self.sync_states.entry(peer_id.clone()).or_default();
        let message = self.document.generate_sync_message(sync_state);
        self.persister
            .set_sync_state(peer_id, sync_state.encode())
            .map_err(Error::PersisterError)?;
        Ok(message)
    }

    /// Receive a sync message from a peer backend.
    ///
    /// Peer id is intentionally low level and up to the user as it can be a DNS name, IP address or
    /// something else.
    ///
    /// This internally retrieves the previous sync state from storage and saves the new one
    /// afterwards.
    pub fn receive_sync_message(
        &mut self,
        peer_id: PeerId,
        message: sync::Message,
    ) -> Result<(), Error<P::Error>> {
        self.close_transaction()?;

        if !self.sync_states.contains_key(&peer_id) {
            if let Some(sync_state) = self
                .persister
                .get_sync_state(&peer_id)
                .map_err(Error::PersisterError)?
            {
                let s = sync::State::decode(&sync_state).map_err(Error::AutomergeDecodeError)?;
                self.sync_states.insert(peer_id.clone(), s);
            }
        }
        let sync_state = self.sync_states.entry(peer_id.clone()).or_default();

        let heads = self.document.get_heads();
        self.document
            .receive_sync_message(sync_state, message)
            .map_err(Error::AutomergeError)?;
        let changes = self.document.get_changes(&heads)?;
        self.persister
            .insert_changes(
                changes
                    .into_iter()
                    .map(|c| (c.actor_id().clone(), c.seq(), c.raw_bytes().to_vec()))
                    .collect(),
            )
            .map_err(Error::PersisterError)?;

        self.persister
            .set_sync_state(peer_id, sync_state.encode())
            .map_err(Error::PersisterError)?;
        Ok(())
    }

    /// Flush any data out to storage returning the number of bytes flushed.
    ///
    /// # Errors
    ///
    /// Returns the error returned by the persister during flushing.
    pub fn flush(&mut self) -> Result<usize, Error<P::Error>> {
        self.close_transaction()?;
        let bytes = self.persister.flush().map_err(Error::PersisterError)?;
        Ok(bytes)
    }

    /// Close any current transaction and write out the changes to disk.
    pub fn close_transaction(&mut self) -> Result<(), Error<P::Error>> {
        for change in self.document.get_changes(&self.saved_heads)? {
            self.persister
                .insert_changes(vec![(
                    change.actor_id().clone(),
                    change.seq(),
                    change.raw_bytes().to_vec(),
                )])
                .map_err(Error::PersisterError)?
        }
        self.saved_heads = self.document.get_heads();
        Ok(())
    }

    /// Close the document.
    ///
    /// This calls flush on the persister and returns it for potential use in other documents.
    ///
    /// # Errors
    ///
    /// Returns the error from flushing.
    pub fn close(mut self) -> Result<P, Error<P::Error>> {
        self.flush()?;
        Ok(self.persister)
    }

    /// Obtain a reference to the persister.
    pub const fn persister(&self) -> &P {
        &self.persister
    }

    /// Reset the sync state for a peer.
    ///
    /// This is typically used when a peer disconnects, we need to reset the sync state for them as
    /// they may come back up with different state.
    pub fn reset_sync_state(&mut self, peer_id: &[u8]) -> Result<(), P::Error> {
        self.sync_states.remove(peer_id);
        self.persister.remove_sync_states(&[peer_id])
    }
}
