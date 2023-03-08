// #![warn(missing_docs)]
#![warn(missing_crate_level_docs)]
#![warn(missing_doc_code_examples)]
// #![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

//! A library for constructing efficient persistent automerge documents.
//!
//! A [`PersistentAutomerge`] wraps an [`automerge::Automerge`] and handles making the changes applied
//! to it durable. This works by persisting every change before it is applied to the document. Then
//! occasionally the user should call `compact` to save the document in a more compact format and
//! cleanup the included changes. This strategy aims to be fast while also being space efficient
//! (up to the user's requirements).
//!
//! ```rust
//! # use automerge_persistent::MemoryPersister;
//! # use automerge_persistent::PersistentAutomerge;
//! let persister = MemoryPersister::default();
//! let doc = PersistentAutomerge::load(persister).unwrap();
//! ```

mod autocommit;
mod mem;
mod persister;

use std::{collections::HashMap, fmt::Debug};

pub use autocommit::PersistentAutoCommit;
use automerge::{
    sync::{self, DecodeStateError, SyncDoc},
    transaction::{CommitOptions, Failure, Observed, Success, Transaction, UnObserved},
    Automerge, AutomergeError, Change, LoadChangeError, OpObserver, op_observer::BranchableObserver,
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

/// Errors that persistent documents can return.
#[derive(Debug, thiserror::Error)]
pub enum Error<E> {
    /// An automerge error.
    #[error(transparent)]
    AutomergeError(#[from] AutomergeError),
    #[error(transparent)]
    AutomergeDecodeError(#[from] DecodeStateError),
    #[error(transparent)]
    AutomergeLoadChangeError(#[from] LoadChangeError),
    /// A persister error.
    #[error(transparent)]
    PersisterError(E),
}

/// Errors that persistent documents can return after a transaction.
#[derive(Debug, thiserror::Error)]
pub enum TransactionError<PE, E> {
    /// A persister error.
    #[error(transparent)]
    PersisterError(PE),
    /// A transaction error
    #[error(transparent)]
    TransactionError(#[from] Failure<E>),
}

pub type TransactionResult<O, Obs, E, PE> = Result<Success<O, Obs>, TransactionError<PE, E>>;

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
    pub const fn document(&self) -> &Automerge {
        &self.document
    }

    pub fn document_mut(&mut self) -> &mut Automerge {
        &mut self.document
    }

    pub fn transact<F, O, E>(&mut self, f: F) -> TransactionResult<O, (), E, P::Error>
    where
        F: FnOnce(&mut Transaction<UnObserved>) -> Result<O, E>,
    {
        let result = self.document.transact(f)?;
        if let Err(e) = self.after_transaction() {
            return Err(TransactionError::PersisterError(e));
        }
        Ok(result)
    }

    fn after_transaction(&mut self) -> Result<(), P::Error> {
        if let Some(change) = self.document.get_last_local_change() {
            self.persister.insert_changes(vec![(
                change.actor_id().clone(),
                change.seq(),
                change.raw_bytes().to_vec(),
            )])?;
        }
        Ok(())
    }

    pub fn transact_with<F, O, E, C, Obs>(
        &mut self,
        c: C,
        f: F,
    ) -> TransactionResult<O, Obs, E, P::Error>
    where
        F: FnOnce(&mut Transaction<'_, Observed<Obs>>) -> Result<O, E>,
        C: FnOnce(&O) -> CommitOptions,
        Obs: OpObserver + BranchableObserver + Default,
    {
        let result = self.document.transact_observed_with(c, f)?;
        if let Err(e) = self.after_transaction() {
            return Err(TransactionError::PersisterError(e));
        }
        Ok(result)
    }

    /// Apply changes to this document.
    pub fn apply_changes(
        &mut self,
        changes: impl IntoIterator<Item = Change>,
    ) -> Result<(), Error<P::Error>> {
        self.apply_changes_with::<_, ()>(changes, None)
    }

    pub fn apply_changes_with<I: IntoIterator<Item = Change>, Obs: OpObserver>(
        &mut self,
        changes: I,
        op_observer: Option<&mut Obs>,
    ) -> Result<(), Error<P::Error>> {
        let mut to_persist = vec![];
        self.document.apply_changes_with(
            changes.into_iter().map(|change| {
                to_persist.push((
                    change.actor_id().clone(),
                    change.seq(),
                    change.raw_bytes().to_vec(),
                ));
                change
            }),
            op_observer,
        )?;
        self.persister
            .insert_changes(to_persist)
            .map_err(Error::PersisterError)?;
        Ok(())
    }

    /// Load the persisted changes (both individual changes and a document) from storage and
    /// rebuild the Document.
    ///
    /// ```rust
    /// # use automerge_persistent::MemoryPersister;
    /// # use automerge_persistent::PersistentAutomerge;
    /// let persister = MemoryPersister::default();
    /// let doc = PersistentAutomerge::load(persister).unwrap();
    /// ```
    pub fn load(persister: P) -> Result<Self, Error<P::Error>> {
        let document = persister.get_document().map_err(Error::PersisterError)?;
        let mut doc = if let Some(document) = document {
            Automerge::load(&document).map_err(Error::AutomergeError)?
        } else {
            Automerge::default()
        };

        let change_bytes = persister.get_changes().map_err(Error::PersisterError)?;

        let mut changes = Vec::new();
        for change_bytes in change_bytes {
            changes.push(Change::from_bytes(change_bytes).map_err(Error::AutomergeLoadChangeError)?)
        }

        doc
            .apply_changes(changes)
            .map_err(Error::AutomergeError)?;
        Ok(Self {
            document: doc,
            sync_states: HashMap::new(),
            persister,
        })
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
    /// # let mut document = PersistentAutomerge::load(persister).unwrap();
    /// document.compact(&[]).unwrap();
    /// ```
    pub fn compact(&mut self, old_peer_ids: &[&[u8]]) -> Result<(), Error<P::Error>> {
        let saved_document = self.document.save();
        let changes = self.document.get_changes(&[])?;
        self.persister
            .set_document(saved_document)
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
    /// # let mut document = PersistentAutomerge::load(persister).unwrap();
    /// let message = document.generate_sync_message(vec![]).unwrap();
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
        message: sync::Message,
    ) -> Result<(), Error<P::Error>> {
        self.receive_sync_message_with(peer_id, message, &mut ())
    }

    /// Receive a sync message from a peer document.
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
        op_observer: &mut Obs,
    ) -> Result<(), Error<P::Error>> {
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
            .receive_sync_message_with(sync_state, message, op_observer)
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
    pub const fn persister(&self) -> &P {
        &self.persister
    }

    /// Obtain a mut reference to the persister.
    pub fn persister_mut(&mut self) -> &mut P {
        &mut self.persister
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
