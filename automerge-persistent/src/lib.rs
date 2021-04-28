#![warn(missing_docs)]
#![warn(missing_crate_level_docs)]
#![warn(missing_doc_code_examples)]

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
//! # fn main() -> Result<(), automerge_persistent::PersistentBackendError<std::convert::Infallible>> {
//! let persister = MemoryPersister::default();
//! let backend = PersistentBackend::load(persister)?;
//! # Ok(())
//! # }
//! ```

mod mem;

use std::{collections::HashMap, error::Error, fmt::Debug};

use automerge::Change;
use automerge_backend::{AutomergeError, SyncMessage, SyncState};
use automerge_protocol::{ActorId, ChangeHash, Patch, UncompressedChange};
pub use mem::MemoryPersister;

/// A Persister persists both changes and documents to durable storage.
///
/// In the event of a power loss changes should still be around for loading after. It is up to the
/// implementation to decide on trade-offs regarding how often to fsync for example.
///
/// Changes are identified by a pair of actor_id and sequence_number. This uniquely identifies a
/// change and so is suitable for use as a key in the implementation.
///
/// Documents are saved automerge Backends so are more compact than the raw changes they represent.
pub trait Persister {
    /// The error type that the operations can produce
    type Error: Debug + Error + 'static;

    /// Returns all of the changes that have been persisted through this persister.
    /// Ordering is not specified as the automerge Backend should handle that.
    fn get_changes(&self) -> Result<Vec<Vec<u8>>, Self::Error>;

    /// Inserts the given change at the unique address specified by the actor_id and sequence_number.
    fn insert_changes(&mut self, changes: Vec<(ActorId, u64, Vec<u8>)>) -> Result<(), Self::Error>;

    /// Removes the change at the unique address specified by the actor_id and sequence_number.
    ///
    /// If the change does not exist this should not return an error.
    fn remove_changes(&mut self, changes: Vec<(&ActorId, u64)>) -> Result<(), Self::Error>;

    /// Returns the document, if one has been persisted previously.
    fn get_document(&self) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Sets the document to the given data.
    fn set_document(&mut self, data: Vec<u8>) -> Result<(), Self::Error>;

    /// Returns the sync state for the given peer if one exists.
    ///
    /// A peer id corresponds to an instance of a backend and may be serving multiple frontends so
    /// we cannot have it work on ActorIds.
    fn get_sync_state(&self, peer_id: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Sets the sync state for the given peer.
    ///
    /// A peer id corresponds to an instance of a backend and may be serving multiple frontends so
    /// we cannot have it work on ActorIds.
    fn set_sync_state(&mut self, peer_id: Vec<u8>, sync_state: Vec<u8>) -> Result<(), Self::Error>;

    /// Removes the sync states associated with the given peer_ids.
    fn remove_sync_states(&mut self, peer_ids: &[&[u8]]) -> Result<(), Self::Error>;

    /// Returns the list of peer ids with stored SyncStates.
    ///
    /// This is intended for use by users to see what peer_ids are taking space so that they can be
    /// removed during a compaction.
    fn get_peer_ids(&self) -> Result<Vec<Vec<u8>>, Self::Error>;
}

/// Errors that persistent backends can return.
#[derive(Debug, thiserror::Error)]
pub enum PersistentBackendError<E>
where
    E: Debug + Error + 'static,
{
    /// An internal automerge error.
    #[error(transparent)]
    AutomergeError(#[from] AutomergeError),
    /// A persister error.
    #[error(transparent)]
    PersisterError(E),
}

type PeerId = Vec<u8>;

/// A wrapper for a persister and an automerge Backend.
#[derive(Debug)]
pub struct PersistentBackend<P: Persister + Debug> {
    backend: automerge::Backend,
    sync_states: HashMap<PeerId, SyncState>,
    persister: P,
}

impl<P> PersistentBackend<P>
where
    P: Persister + Debug,
{
    /// Load the persisted changes (both individual changes and a document) from storage and
    /// rebuild the Backend.
    ///
    /// ```rust
    /// # use automerge_persistent::MemoryPersister;
    /// # use automerge_persistent::PersistentBackend;
    /// let persister = MemoryPersister::default();
    /// let backend = PersistentBackend::load(persister).unwrap();
    /// ```
    pub fn load(persister: P) -> Result<Self, PersistentBackendError<P::Error>> {
        let document = persister
            .get_document()
            .map_err(PersistentBackendError::PersisterError)?;
        let mut backend = if let Some(document) = document {
            automerge::Backend::load(document)?
        } else {
            automerge::Backend::init()
        };

        let change_bytes = persister
            .get_changes()
            .map_err(PersistentBackendError::PersisterError)?;
        let mut changes = Vec::new();
        for change_bytes in change_bytes {
            changes.push(Change::from_bytes(change_bytes)?)
        }

        backend
            .apply_changes(changes)
            .map_err(PersistentBackendError::AutomergeError)?;
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
    /// # let mut backend = PersistentBackend::load(persister).unwrap();
    /// let patch = backend.apply_changes(vec![]).unwrap();
    /// ```
    pub fn apply_changes(
        &mut self,
        changes: Vec<Change>,
    ) -> Result<Patch, PersistentBackendError<P::Error>> {
        self.persister
            .insert_changes(
                changes
                    .iter()
                    .map(|c| (c.actor_id().clone(), c.seq, c.raw_bytes().to_vec()))
                    .collect(),
            )
            .map_err(PersistentBackendError::PersisterError)?;
        self.backend
            .apply_changes(changes)
            .map_err(PersistentBackendError::AutomergeError)
    }

    /// Apply a local change, typically from a local frontend.
    pub fn apply_local_change(
        &mut self,
        change: UncompressedChange,
    ) -> Result<Patch, PersistentBackendError<P::Error>> {
        let (patch, change) = self.backend.apply_local_change(change)?;
        self.persister
            .insert_changes(vec![(
                change.actor_id().clone(),
                change.seq,
                change.raw_bytes().to_vec(),
            )])
            .map_err(PersistentBackendError::PersisterError)?;
        Ok(patch)
    }

    /// Compact the storage.
    ///
    /// This first obtains the changes currently in the backend, saves the backend and persists the
    /// saved document. We then can remove the previously obtained changes one by one.
    ///
    /// It also clears out the storage used up by old sync states for peers by removing those given
    /// in old_peers.
    ///
    /// ```rust
    /// # use automerge_persistent::MemoryPersister;
    /// # use automerge_persistent::PersistentBackend;
    /// # let persister = MemoryPersister::default();
    /// # let mut backend = PersistentBackend::load(persister).unwrap();
    /// backend.compact(&[]).unwrap();
    /// ```
    pub fn compact(
        &mut self,
        old_peer_ids: &[&[u8]],
    ) -> Result<(), PersistentBackendError<P::Error>> {
        let changes = self.backend.get_changes(&[]);
        let saved_backend = self.backend.save()?;
        self.persister
            .set_document(saved_backend)
            .map_err(PersistentBackendError::PersisterError)?;
        self.persister
            .remove_changes(changes.into_iter().map(|c| (c.actor_id(), c.seq)).collect())
            .map_err(PersistentBackendError::PersisterError)?;
        self.persister
            .remove_sync_states(old_peer_ids)
            .map_err(PersistentBackendError::PersisterError)?;
        Ok(())
    }

    /// Get a patch from the current data in the backend to populate a frontend.
    ///
    /// ```rust
    /// # use automerge_persistent::MemoryPersister;
    /// # use automerge_persistent::PersistentBackend;
    /// # let persister = MemoryPersister::default();
    /// # let mut backend = PersistentBackend::load(persister).unwrap();
    /// let patch = backend.get_patch().unwrap();
    /// ```
    pub fn get_patch(&self) -> Result<Patch, PersistentBackendError<P::Error>> {
        self.backend
            .get_patch()
            .map_err(PersistentBackendError::AutomergeError)
    }

    /// Get the changes performed by the given actor_id.
    pub fn get_changes_for_actor_id(
        &self,
        actor_id: &ActorId,
    ) -> Result<Vec<&Change>, PersistentBackendError<P::Error>> {
        self.backend
            .get_changes_for_actor_id(actor_id)
            .map_err(PersistentBackendError::AutomergeError)
    }

    /// Get all changes that have the given dependencies (transitively obtains more recent ones).
    ///
    /// ```rust
    /// # use automerge_persistent::MemoryPersister;
    /// # use automerge_persistent::PersistentBackend;
    /// # let persister = MemoryPersister::default();
    /// # let mut backend = PersistentBackend::load(persister).unwrap();
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
    /// # let mut backend = PersistentBackend::load(persister).unwrap();
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
    /// # let mut backend = PersistentBackend::load(persister).unwrap();
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
    /// # let mut backend = PersistentBackend::load(persister).unwrap();
    /// let message = backend.generate_sync_message(vec![]).unwrap();
    /// ```
    pub fn generate_sync_message(
        &mut self,
        peer_id: PeerId,
    ) -> Result<Option<SyncMessage>, PersistentBackendError<P::Error>> {
        if !self.sync_states.contains_key(&peer_id) {
            if let Some(sync_state) = self
                .persister
                .get_sync_state(&peer_id)
                .map_err(PersistentBackendError::PersisterError)?
            {
                let s = SyncState::decode(&sync_state)?;
                self.sync_states.insert(peer_id.clone(), s);
            }
        }
        let sync_state = self.sync_states.entry(peer_id.clone()).or_default();
        let message = self.backend.generate_sync_message(sync_state);
        self.persister
            .set_sync_state(
                peer_id,
                sync_state
                    .clone()
                    .encode()
                    .map_err(PersistentBackendError::AutomergeError)?,
            )
            .map_err(PersistentBackendError::PersisterError)?;
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
    ) -> Result<Option<Patch>, PersistentBackendError<P::Error>> {
        if !self.sync_states.contains_key(&peer_id) {
            if let Some(sync_state) = self
                .persister
                .get_sync_state(&peer_id)
                .map_err(PersistentBackendError::PersisterError)?
            {
                let s = SyncState::decode(&sync_state)?;
                self.sync_states.insert(peer_id.clone(), s);
            }
        }
        let sync_state = self.sync_states.entry(peer_id.clone()).or_default();
        let patch = self
            .backend
            .receive_sync_message(sync_state, message)
            .map_err(PersistentBackendError::AutomergeError)?;
        self.persister
            .set_sync_state(
                peer_id,
                sync_state
                    .clone()
                    .encode()
                    .map_err(PersistentBackendError::AutomergeError)?,
            )
            .map_err(PersistentBackendError::PersisterError)?;
        Ok(patch)
    }
}
