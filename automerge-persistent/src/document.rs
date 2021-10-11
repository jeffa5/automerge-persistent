use std::{collections::HashMap, fmt::Debug};

use automerge::{
    value_ref::RootRef, Automerge, AutomergeBuilder, AutomergeError, Backend, Change, Frontend,
    MutableDocument, Path, Value,
};
use automerge_backend::{SyncMessage, SyncState};
use automerge_protocol::{ChangeHash, OpId};

use crate::Persister;

/// Errors that persistent backends can return.
#[derive(Debug, thiserror::Error)]
pub enum Error<E> {
    /// An automerge error.
    #[error(transparent)]
    AutomergeError(#[from] AutomergeError),
    /// A persister error.
    #[error(transparent)]
    PersisterError(E),
    /// An error resulting from a user-provided change function.
    #[error("change error: {0}")]
    ChangeError(Box<dyn std::error::Error>),
}

type PeerId = Vec<u8>;

/// A wrapper for a persister and an automerge document.
#[derive(Debug)]
pub struct PersistentAutomerge<P> {
    automerge: Automerge,
    sync_states: HashMap<PeerId, SyncState>,
    persister: P,
}

impl<P> PersistentAutomerge<P>
where
    P: Persister + 'static,
{
    /// Load the persisted changes (both individual changes and whole document) from storage and
    /// rebuild the document.
    ///
    /// ```rust
    /// # use automerge_persistent::MemoryPersister;
    /// # use automerge_persistent::PersistentAutomerge;
    /// let persister = MemoryPersister::default();
    /// let document = PersistentAutomerge::<_>::load(persister).unwrap();
    /// ```
    pub fn load(persister: P) -> Result<Self, Error<P::Error>> {
        let document = persister.get_document().map_err(Error::PersisterError)?;
        let mut automerge = if let Some(document) = document {
            Automerge::load(document).map_err(Error::AutomergeError)?
        } else {
            Automerge::default()
        };

        let change_bytes = persister.get_changes().map_err(Error::PersisterError)?;

        let mut changes = Vec::new();
        for change_bytes in change_bytes {
            changes.push(
                Change::from_bytes(change_bytes)
                    .map_err(|e| Error::AutomergeError(AutomergeError::BackendError(e.into())))?,
            );
        }

        automerge
            .apply_changes(changes)
            .map_err(Error::AutomergeError)?;
        Ok(Self {
            automerge,
            sync_states: HashMap::new(),
            persister,
        })
    }

    pub fn load_with_frontend(persister: P, frontend: Frontend) -> Result<Self, Error<P::Error>> {
        let document = persister.get_document().map_err(Error::PersisterError)?;
        let mut automerge = if let Some(document) = document {
            AutomergeBuilder::default()
                .with_frontend(frontend)
                .with_backend(
                    Backend::load(document)
                        .map_err(|e| Error::AutomergeError(AutomergeError::BackendError(e)))?,
                )
                .build()
        } else {
            AutomergeBuilder::default().with_frontend(frontend).build()
        };

        let change_bytes = persister.get_changes().map_err(Error::PersisterError)?;

        let mut changes = Vec::new();
        for change_bytes in change_bytes {
            changes.push(
                Change::from_bytes(change_bytes)
                    .map_err(|e| Error::AutomergeError(AutomergeError::BackendError(e.into())))?,
            );
        }

        automerge
            .apply_changes(changes)
            .map_err(Error::AutomergeError)?;
        Ok(Self {
            automerge,
            sync_states: HashMap::new(),
            persister,
        })
    }

    pub fn state(&mut self) -> &Value {
        self.automerge.state()
    }

    pub fn value_ref(&self) -> RootRef {
        self.automerge.value_ref()
    }

    pub fn change<F, O, E>(
        &mut self,
        message: Option<String>,
        change_closure: F,
    ) -> Result<O, Error<P::Error>>
    where
        E: std::error::Error + 'static,
        F: FnOnce(&mut dyn MutableDocument) -> Result<O, E>,
    {
        let heads = self.automerge.get_heads();
        let (res, _) = self
            .automerge
            .change(message, change_closure)
            .map_err(|e| Error::ChangeError(Box::new(e)))?;
        let changes = self.automerge.get_changes(&heads);
        self.persister
            .insert_changes(
                changes
                    .into_iter()
                    .map(|c| (c.actor_id().clone(), c.seq, c.raw_bytes().to_vec()))
                    .collect(),
            )
            .map_err(Error::PersisterError)?;
        Ok(res)
    }

    /// Compact the storage.
    ///
    /// This first obtains the changes currently in the document, saves the document and persists the
    /// saved document. We then can remove the previously obtained changes one by one.
    ///
    /// It also clears out the storage used up by old sync states for peers by removing those given
    /// in `old_peers`.
    ///
    /// ```rust
    /// # use automerge_persistent::MemoryPersister;
    /// # use automerge_persistent::PersistentAutomerge;
    /// # let persister = MemoryPersister::default();
    /// # let mut document = PersistentAutomerge::<_>::load(persister).unwrap();
    /// document.compact(&[]).unwrap();
    /// ```
    pub fn compact(&mut self, old_peer_ids: &[&[u8]]) -> Result<(), Error<P::Error>> {
        let changes = self.automerge.get_changes(&[]);
        let saved_document = self
            .automerge
            .save()
            .map_err(|e| Error::AutomergeError(AutomergeError::BackendError(e)))?;
        self.persister
            .set_document(saved_document)
            .map_err(Error::PersisterError)?;
        self.persister
            .remove_changes(changes.into_iter().map(|c| (c.actor_id(), c.seq)).collect())
            .map_err(Error::PersisterError)?;
        self.persister
            .remove_sync_states(old_peer_ids)
            .map_err(Error::PersisterError)?;
        Ok(())
    }

    pub fn get_conflicts(&self, path: &Path) -> Option<HashMap<OpId, Value>> {
        self.automerge.get_conflicts(path)
    }

    pub fn get_value(&self, path: &Path) -> Option<Value> {
        self.automerge.get_value(path)
    }

    /// Get all changes that have the given dependencies (transitively obtains more recent ones).
    ///
    /// ```rust
    /// # use automerge_persistent::MemoryPersister;
    /// # use automerge_persistent::PersistentAutomerge;
    /// # let persister = MemoryPersister::default();
    /// # let mut document = PersistentAutomerge::<_>::load(persister).unwrap();
    /// let all_changes = document.get_changes(&[]);
    /// ```
    pub fn get_changes(&self, have_deps: &[ChangeHash]) -> Vec<&Change> {
        self.automerge.get_changes(have_deps)
    }

    /// Get the current heads of the hash graph (changes without successors).
    ///
    /// ```rust
    /// # use automerge_persistent::MemoryPersister;
    /// # use automerge_persistent::PersistentAutomerge;
    /// # let persister = MemoryPersister::default();
    /// # let mut document = PersistentAutomerge::<_>::load(persister).unwrap();
    /// let heads = document.get_heads();
    /// ```
    pub fn get_heads(&self) -> Vec<ChangeHash> {
        self.automerge.get_heads()
    }

    /// Generate a sync message to be sent to a peer document.
    ///
    /// Peer id is intentionally low level and up to the user as it can be a DNS name, IP address or
    /// something else.
    ///
    /// This internally retrieves the previous sync state from storage and saves the new one
    /// afterwards.
    ///
    /// ```rust
    /// # use automerge_persistent::MemoryPersister;
    /// # use automerge_persistent::PersistentAutomerge;
    /// # let persister = MemoryPersister::default();
    /// # let mut document = PersistentAutomerge::<_>::load(persister).unwrap();
    /// let message = document.generate_sync_message(vec![]).unwrap();
    /// ```
    pub fn generate_sync_message(
        &mut self,
        peer_id: PeerId,
    ) -> Result<Option<SyncMessage>, Error<P::Error>> {
        if !self.sync_states.contains_key(&peer_id) {
            if let Some(sync_state) = self
                .persister
                .get_sync_state(&peer_id)
                .map_err(Error::PersisterError)?
            {
                let s = SyncState::decode(&sync_state)
                    .map_err(|e| Error::AutomergeError(AutomergeError::BackendError(e.into())))?;
                self.sync_states.insert(peer_id.clone(), s);
            }
        }
        let sync_state = self.sync_states.entry(peer_id.clone()).or_default();
        let message = self.automerge.generate_sync_message(sync_state);
        self.persister
            .set_sync_state(
                peer_id,
                sync_state
                    .encode()
                    .map_err(|e| Error::AutomergeError(AutomergeError::BackendError(e.into())))?,
            )
            .map_err(Error::PersisterError)?;
        Ok(message)
    }

    /// Receive a sync message from a peer document.
    ///
    /// Peer id is intentionally low level and up to the user as it can be a DNS name, IP address or
    /// something else.
    ///
    /// This internally retrieves the previous sync state from storage and saves the new one
    /// afterwards.
    pub fn receive_sync_message(
        &mut self,
        peer_id: PeerId,
        message: SyncMessage,
    ) -> Result<(), Error<P::Error>> {
        if !self.sync_states.contains_key(&peer_id) {
            if let Some(sync_state) = self
                .persister
                .get_sync_state(&peer_id)
                .map_err(Error::PersisterError)?
            {
                let s = SyncState::decode(&sync_state)
                    .map_err(|e| Error::AutomergeError(AutomergeError::BackendError(e.into())))?;
                self.sync_states.insert(peer_id.clone(), s);
            }
        }
        let sync_state = self.sync_states.entry(peer_id.clone()).or_default();

        let heads = self.automerge.get_heads();
        self.automerge
            .receive_sync_message(sync_state, message)
            .map_err(Error::AutomergeError)?;
        let changes = self.automerge.get_changes(&heads);
        self.persister
            .insert_changes(
                changes
                    .into_iter()
                    .map(|c| (c.actor_id().clone(), c.seq, c.raw_bytes().to_vec()))
                    .collect(),
            )
            .unwrap();

        self.persister
            .set_sync_state(
                peer_id,
                sync_state
                    .encode()
                    .map_err(|e| Error::AutomergeError(AutomergeError::BackendError(e.into())))?,
            )
            .map_err(Error::PersisterError)?;
        Ok(())
    }

    /// Flush any data out to storage.
    ///
    /// # Errors
    ///
    /// Returns the error returned by the persister during flushing.
    pub fn flush(&mut self) -> Result<(), P::Error> {
        self.persister.flush()
    }

    /// Close the document.
    ///
    /// This calls flush on the persister and returns it for potential use in other documents.
    ///
    /// # Errors
    ///
    /// Returns the error from flushing.
    pub fn close(mut self) -> Result<P, P::Error> {
        self.flush()?;
        Ok(self.persister)
    }

    /// Obtain a reference to the persister.
    pub fn persister(&self) -> &P {
        &self.persister
    }
}
