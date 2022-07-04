// #![warn(missing_docs)]
#![warn(missing_crate_level_docs)]
#![warn(missing_doc_code_examples)]
// #![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

//! A library for constructing efficient persistent automerge documents.
//!
//! A [`PersistentBackend`] wraps an [`automerge::Backend`] and handles making the changes applied
//! to it durable. This works by persisting every change before it is applied to the backend. Then
//! occasionally the user should call `compact` to save the backend in a more compact format and
//! cleanup the included changes. This strategy aims to be fast while also being space efficient
//! (up to the user's requirements).
//!
//! ```rust
//! # use automerge_persistent::MemoryPersister;
//! # use automerge_persistent::PersistentBackend;
//! # fn main() -> Result<(), automerge_persistent::Error<std::convert::Infallible,
//! automerge_backend::AutomergeError>> {
//! let persister = MemoryPersister::default();
//! let backend = PersistentBackend::<_, automerge::Backend>::load(persister)?;
//! # Ok(())
//! # }
//! ```

mod autocommit;
mod mem;
mod persister;

use std::{collections::HashMap, fmt::Debug};

pub use autocommit::PersistentAutoCommit;
use automerge::{
    sync,
    transaction::{self, Transaction},
    ApplyOptions, Automerge, AutomergeError, Change, OpObserver,
};
pub use mem::MemoryPersister;
pub use persister::Persister;

/// Bytes stored for each of the stored types.
#[derive(Debug, Default, Clone)]
pub struct StoredSizes {
    /// Total bytes stored for all changes.
    pub changes: u64,
    /// Total bytes stored in the document.
    pub document: u64,
    /// Total bytes stored for all sync states.
    pub sync_states: u64,
}

/// Errors that persistent backends can return.
#[derive(Debug, thiserror::Error)]
pub enum Error<E> {
    /// An automerge error.
    #[error(transparent)]
    AutomergeError(#[from] AutomergeError),
    /// A persister error.
    #[error(transparent)]
    PersisterError(E),
}

type PeerId = Vec<u8>;

/// A wrapper for a persister and an automerge document.
#[derive(Debug)]
pub struct PersistentAutomerge<P> {
    document: Automerge,
    sync_states: HashMap<PeerId, sync::State>,
    persister: P,
}

impl<P> PersistentAutomerge<P>
where
    P: Persister + 'static,
{
    pub fn document(&self) -> &Automerge {
        &self.document
    }

    pub fn document_mut(&mut self) -> &mut Automerge {
        &mut self.document
    }

    pub fn transact<F: FnOnce(&mut Transaction) -> Result<O, E>, O, E>(
        &mut self,
        f: F,
    ) -> transaction::Result<O, E> {
        let result = self.document.transact(f)?;
        if let Some(change) = self.document.get_last_local_change() {
            // TODO: remove this unwrap and return the error
            self.persister
                .insert_changes(vec![(
                    change.actor_id().clone(),
                    change.seq,
                    change.raw_bytes().to_vec(),
                )])
                .expect("Failed to save change from transaction");
        }
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
            Automerge::load(&document).map_err(Error::AutomergeError)?
        } else {
            Automerge::default()
        };

        let change_bytes = persister.get_changes().map_err(Error::PersisterError)?;

        let mut changes = Vec::new();
        for change_bytes in change_bytes {
            changes.push(
                Change::from_bytes(change_bytes).map_err(|e| Error::AutomergeError(e.into()))?,
            )
        }

        backend
            .apply_changes(changes)
            .map_err(Error::AutomergeError)?;
        Ok(Self {
            document: backend,
            sync_states: HashMap::new(),
            persister,
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
        let changes = self.document.get_changes(&[])?;
        self.persister
            .set_document(saved_backend)
            .map_err(Error::PersisterError)?;
        self.persister
            .remove_changes(changes.into_iter().map(|c| (c.actor_id(), c.seq)).collect())
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
        if !self.sync_states.contains_key(&peer_id) {
            if let Some(sync_state) = self
                .persister
                .get_sync_state(&peer_id)
                .map_err(Error::PersisterError)?
            {
                let s = sync::State::decode(&sync_state)
                    .map_err(|e| Error::AutomergeError(e.into()))?;
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
        self.receive_sync_message_with(peer_id, message, ApplyOptions::<()>::default())
    }

    /// Receive a sync message from a peer backend.
    ///
    /// Peer id is intentionally low level and up to the user as it can be a DNS name, IP address or
    /// something else.
    ///
    /// This internally retrieves the previous sync state from storage and saves the new one
    /// afterwards.
    pub fn receive_sync_message_with<Obs: OpObserver>(
        &mut self,
        peer_id: PeerId,
        message: sync::Message,
        options: ApplyOptions<Obs>,
    ) -> Result<(), Error<P::Error>> {
        if !self.sync_states.contains_key(&peer_id) {
            if let Some(sync_state) = self
                .persister
                .get_sync_state(&peer_id)
                .map_err(Error::PersisterError)?
            {
                let s = sync::State::decode(&sync_state)
                    .map_err(|e| Error::AutomergeError(e.into()))?;
                self.sync_states.insert(peer_id.clone(), s);
            }
        }
        let sync_state = self.sync_states.entry(peer_id.clone()).or_default();

        let heads = self.document.get_heads();
        let patch = self
            .document
            .receive_sync_message_with(sync_state, message, options)
            .map_err(Error::AutomergeError)?;
        let changes = self.document.get_changes(&heads)?;
        self.persister
            .insert_changes(
                changes
                    .into_iter()
                    .map(|c| (c.actor_id().clone(), c.seq, c.raw_bytes().to_vec()))
                    .collect(),
            )
            .map_err(Error::PersisterError)?;

        self.persister
            .set_sync_state(peer_id, sync_state.encode())
            .map_err(Error::PersisterError)?;
        Ok(patch)
    }

    /// Flush any data out to storage returning the number of bytes flushed.
    ///
    /// # Errors
    ///
    /// Returns the error returned by the persister during flushing.
    pub fn flush(&mut self) -> Result<usize, P::Error> {
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

    /// Reset the sync state for a peer.
    ///
    /// This is typically used when a peer disconnects, we need to reset the sync state for them as
    /// they may come back up with different state.
    pub fn reset_sync_state(&mut self, peer_id: &[u8]) -> Result<(), P::Error> {
        self.sync_states.remove(peer_id);
        self.persister.remove_sync_states(&[peer_id])
    }
}
