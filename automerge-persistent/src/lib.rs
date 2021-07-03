#![warn(missing_docs)]
#![warn(missing_crate_level_docs)]
#![warn(missing_doc_code_examples)]
#![warn(clippy::pedantic)]
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

mod backend;
mod mem;
mod persister;

use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{Arc, Mutex},
};

use automerge::Change;
use automerge_backend::{AutomergeError, ChangeEventHandler, EventHandler, SyncMessage, SyncState};
use automerge_protocol::{ActorId, ChangeHash, Patch};
pub use backend::Backend;
pub use mem::MemoryPersister;
pub use persister::Persister;

/// Bytes stored for each of the stored types.
#[derive(Debug, Default, Clone)]
pub struct StoredSizes {
    /// Total bytes stored for all changes.
    pub changes: usize,
    /// Total bytes stored in the document.
    pub document: usize,
    /// Total bytes stored for all sync states.
    pub sync_states: usize,
}

/// Errors that persistent backends can return.
#[derive(Debug, thiserror::Error)]
pub enum Error<E, B>
where
    E: Debug + std::error::Error + 'static,
    B: std::error::Error + 'static,
{
    /// An internal backend error.
    #[error(transparent)]
    BackendError(B),
    /// An automerge error.
    #[error(transparent)]
    AutomergeError(#[from] AutomergeError),
    /// A persister error.
    #[error(transparent)]
    PersisterError(E),
}

type PeerId = Vec<u8>;

/// A wrapper for a persister and an automerge Backend.
#[derive(Debug)]
pub struct PersistentBackend<P: Persister + Debug, B: Backend> {
    backend: B,
    sync_states: HashMap<PeerId, SyncState>,
    persister: Arc<Mutex<P>>,
}

impl<P, B> PersistentBackend<P, B>
where
    P: Persister + Debug + 'static,
    B: Backend,
{
    /// Load the persisted changes (both individual changes and a document) from storage and
    /// rebuild the Backend.
    ///
    /// ```rust
    /// # use automerge_persistent::MemoryPersister;
    /// # use automerge_persistent::PersistentBackend;
    /// let persister = MemoryPersister::default();
    /// let backend = PersistentBackend::<_, automerge::Backend>::load(persister).unwrap();
    /// ```
    pub fn load(persister: P) -> Result<Self, Error<P::Error, B::Error>> {
        let document = persister.get_document().map_err(Error::PersisterError)?;
        let mut backend = if let Some(document) = document {
            B::load(document).map_err(Error::BackendError)?
        } else {
            B::default()
        };

        let change_bytes = persister.get_changes().map_err(Error::PersisterError)?;

        let persister = Arc::new(Mutex::new(persister));
        let persister_clone = persister.clone();

        backend.add_event_handler(EventHandler::BeforeApplyChange(ChangeEventHandler(
            Box::new(move |change| {
                persister_clone
                    .lock()
                    .unwrap()
                    .insert_changes(vec![(
                        change.actor_id().clone(),
                        change.seq,
                        change.raw_bytes().to_vec(),
                    )])
                    .unwrap();
            }),
        )));

        let mut changes = Vec::new();
        for change_bytes in change_bytes {
            changes.push(
                Change::from_bytes(change_bytes).map_err(|e| Error::AutomergeError(e.into()))?,
            )
        }

        backend
            .apply_changes(changes)
            .map_err(Error::BackendError)?;
        Ok(Self {
            backend,
            sync_states: HashMap::new(),
            persister,
        })
    }

    /// Apply a sequence of changes, typically from a remote backend.
    ///
    /// ```rust
    /// # use automerge_persistent::MemoryPersister;
    /// # use automerge_persistent::PersistentBackend;
    /// # let persister = MemoryPersister::default();
    /// # let mut backend = PersistentBackend::<_, automerge::Backend>::load(persister).unwrap();
    /// let patch = backend.apply_changes(vec![]).unwrap();
    /// ```
    pub fn apply_changes(
        &mut self,
        changes: Vec<Change>,
    ) -> Result<Patch, Error<P::Error, B::Error>> {
        self.backend
            .apply_changes(changes)
            .map_err(Error::BackendError)
    }

    /// Apply a local change, typically from a local frontend.
    pub fn apply_local_change(
        &mut self,
        change: automerge_protocol::Change,
    ) -> Result<Patch, Error<P::Error, B::Error>> {
        let (patch, _) = self
            .backend
            .apply_local_change(change)
            .map_err(Error::BackendError)?;
        Ok(patch)
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
    pub fn compact(&mut self, old_peer_ids: &[&[u8]]) -> Result<(), Error<P::Error, B::Error>> {
        let changes = self.backend.get_changes(&[]);
        let saved_backend = self.backend.save().map_err(Error::BackendError)?;
        let mut persister = self.persister.lock().unwrap();
        persister
            .set_document(saved_backend)
            .map_err(Error::PersisterError)?;
        persister
            .remove_changes(changes.into_iter().map(|c| (c.actor_id(), c.seq)).collect())
            .map_err(Error::PersisterError)?;
        persister
            .remove_sync_states(old_peer_ids)
            .map_err(Error::PersisterError)?;
        Ok(())
    }

    /// Get a patch from the current data in the backend to populate a frontend.
    ///
    /// ```rust
    /// # use automerge_persistent::MemoryPersister;
    /// # use automerge_persistent::PersistentBackend;
    /// # let persister = MemoryPersister::default();
    /// # let mut backend = PersistentBackend::<_, automerge::Backend>::load(persister).unwrap();
    /// let patch = backend.get_patch().unwrap();
    /// ```
    pub fn get_patch(&self) -> Result<Patch, Error<P::Error, B::Error>> {
        self.backend.get_patch().map_err(Error::BackendError)
    }

    /// Get the changes performed by the given `actor_id`.
    pub fn get_changes_for_actor_id(
        &self,
        actor_id: &ActorId,
    ) -> Result<Vec<&Change>, Error<P::Error, B::Error>> {
        self.backend
            .get_changes_for_actor_id(actor_id)
            .map_err(Error::BackendError)
    }

    /// Get all changes that have the given dependencies (transitively obtains more recent ones).
    ///
    /// ```rust
    /// # use automerge_persistent::MemoryPersister;
    /// # use automerge_persistent::PersistentBackend;
    /// # let persister = MemoryPersister::default();
    /// # let mut backend = PersistentBackend::<_, automerge::Backend>::load(persister).unwrap();
    /// let all_changes = backend.get_changes(&[]);
    /// ```
    pub fn get_changes(&self, have_deps: &[ChangeHash]) -> Vec<&Change> {
        self.backend.get_changes(have_deps)
    }

    /// Get the missing dependencies in the hash graph that are required to be able to apply some
    /// pending changes.
    ///
    /// This may not give all hashes required as multiple changes in a sequence could be missing.
    ///
    /// ```rust
    /// # use automerge_persistent::MemoryPersister;
    /// # use automerge_persistent::PersistentBackend;
    /// # let persister = MemoryPersister::default();
    /// # let mut backend = PersistentBackend::<_, automerge::Backend>::load(persister).unwrap();
    /// let all_missing_changes = backend.get_missing_deps(&[]);
    /// ```
    pub fn get_missing_deps(&self, heads: &[ChangeHash]) -> Vec<ChangeHash> {
        self.backend.get_missing_deps(heads)
    }

    /// Get the current heads of the hash graph (changes without successors).
    ///
    /// ```rust
    /// # use automerge_persistent::MemoryPersister;
    /// # use automerge_persistent::PersistentBackend;
    /// # let persister = MemoryPersister::default();
    /// # let mut backend = PersistentBackend::<_, automerge::Backend>::load(persister).unwrap();
    /// let heads = backend.get_heads();
    /// ```
    pub fn get_heads(&self) -> Vec<ChangeHash> {
        self.backend.get_heads()
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
    ) -> Result<Option<SyncMessage>, Error<P::Error, B::Error>> {
        if !self.sync_states.contains_key(&peer_id) {
            if let Some(sync_state) = self
                .persister
                .lock()
                .unwrap()
                .get_sync_state(&peer_id)
                .map_err(Error::PersisterError)?
            {
                let s =
                    SyncState::decode(&sync_state).map_err(|e| Error::AutomergeError(e.into()))?;
                self.sync_states.insert(peer_id.clone(), s);
            }
        }
        let sync_state = self.sync_states.entry(peer_id.clone()).or_default();
        let message = self
            .backend
            .generate_sync_message(sync_state)
            .map_err(Error::BackendError)?;
        self.persister
            .lock()
            .unwrap()
            .set_sync_state(
                peer_id,
                sync_state
                    .encode()
                    .map_err(|e| Error::AutomergeError(e.into()))?,
            )
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
        message: SyncMessage,
    ) -> Result<Option<Patch>, Error<P::Error, B::Error>> {
        if !self.sync_states.contains_key(&peer_id) {
            if let Some(sync_state) = self
                .persister
                .lock()
                .unwrap()
                .get_sync_state(&peer_id)
                .map_err(Error::PersisterError)?
            {
                let s =
                    SyncState::decode(&sync_state).map_err(|e| Error::AutomergeError(e.into()))?;
                self.sync_states.insert(peer_id.clone(), s);
            }
        }
        let sync_state = self.sync_states.entry(peer_id.clone()).or_default();
        let patch = self
            .backend
            .receive_sync_message(sync_state, message)
            .map_err(Error::BackendError)?;
        self.persister
            .lock()
            .unwrap()
            .set_sync_state(
                peer_id,
                sync_state
                    .encode()
                    .map_err(|e| Error::AutomergeError(e.into()))?,
            )
            .map_err(Error::PersisterError)?;
        Ok(patch)
    }

    /// Flush any data out to storage.
    ///
    /// # Errors
    ///
    /// Returns the error returned by the persister during flushing.
    pub fn flush(&mut self) -> Result<(), P::Error> {
        self.persister.lock().unwrap().flush()
    }
}
