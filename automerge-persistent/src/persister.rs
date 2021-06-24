use std::error::Error;

use automerge_protocol::ActorId;

use crate::StoredSizes;

/// A Persister persists both changes and documents to durable storage.
///
/// In the event of a power loss changes should still be around for loading after. It is up to the
/// implementation to decide on trade-offs regarding how often to fsync for example.
///
/// Changes are identified by a pair of `actor_id` and `sequence_number`. This uniquely identifies a
/// change and so is suitable for use as a key in the implementation.
///
/// Documents are saved automerge Backends so are more compact than the raw changes they represent.
#[async_trait::async_trait]
pub trait Persister {
    /// The error type that the operations can produce
    type Error: Error + 'static;

    /// Returns all of the changes that have been persisted through this persister.
    /// Ordering is not specified as the automerge Backend should handle that.
    fn get_changes(&self) -> Result<Vec<Vec<u8>>, Self::Error>;

    /// Inserts the given change at the unique address specified by the `actor_id` and `sequence_number`.
    fn insert_changes(&mut self, changes: Vec<(ActorId, u64, Vec<u8>)>) -> Result<(), Self::Error>;

    /// Removes the change at the unique address specified by the `actor_id` and `sequence_number`.
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
    /// we cannot have it work on `ActorIds`.
    fn get_sync_state(&self, peer_id: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Sets the sync state for the given peer.
    ///
    /// A peer id corresponds to an instance of a backend and may be serving multiple frontends so
    /// we cannot have it work on `ActorIds`.
    fn set_sync_state(&mut self, peer_id: Vec<u8>, sync_state: Vec<u8>) -> Result<(), Self::Error>;

    /// Removes the sync states associated with the given `peer_ids`.
    fn remove_sync_states(&mut self, peer_ids: &[&[u8]]) -> Result<(), Self::Error>;

    /// Returns the list of peer ids with stored `SyncStates`.
    ///
    /// This is intended for use by users to see what `peer_ids` are taking space so that they can be
    /// removed during a compaction.
    fn get_peer_ids(&self) -> Result<Vec<Vec<u8>>, Self::Error>;

    /// Returns the sizes components being stored consume.
    ///
    /// This can be used as an indicator of when to compact the storage.
    fn sizes(&self) -> StoredSizes;

    /// Flush the data out to disk.
    fn flush(&mut self) -> Result<(), Self::Error>;

    /// Flush the data out to disk.
    async fn flush_async(&mut self) -> Result<(), Self::Error>;
}
