#![warn(missing_docs)]
#![warn(missing_crate_level_docs)]
#![warn(missing_doc_code_examples)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

//! A persister targetting [RocksDB](https://rocksdb.org/).
//!
//! # Single persister
//!
//! ```rust
//! # use automerge_persistent::PersistentBackend;
//! # use automerge_persistent_rocksdb::RocksDbPersister;
//! # use automerge_persistent_rocksdb::RocksDbPersisterError;
//! # use std::sync::{Arc, Mutex};
//! # fn main() -> Result<(), automerge_persistent::Error<RocksDbPersisterError,
//! automerge_backend::AutomergeError>> {
//! let db = Arc::new(Mutex::new(rocksdb::DB::open_default("/tmp/test.rocksdb").map_err(RocksDbPersisterError::RocksDbError)?));
//!
//! let persister = RocksDbPersister::new(
//!     db,
//!     "changes".to_owned(),
//!     "documents".to_owned(),
//!     "sync-states".to_owned(),
//!     String::new(),
//! )?;
//! let backend = PersistentBackend::<_, automerge::Backend>::load(persister)?;
//! # Ok(())
//! # }
//! ```
//!
//! # Multiple persisters sharing the same db through a mutex
//!
//! ```rust
//! # use automerge_persistent::PersistentBackend;
//! # use automerge_persistent_rocksdb::RocksDbPersister;
//! # use automerge_persistent_rocksdb::RocksDbPersisterError;
//! # use std::sync::{Arc, Mutex};
//! # fn main() -> Result<(), automerge_persistent::Error<RocksDbPersisterError,
//! automerge_backend::AutomergeError>> {
//! let db = Arc::new(Mutex::new(rocksdb::DB::open_default("/tmp/test.rocksdb").map_err(RocksDbPersisterError::RocksDbError)?));
//!
//! let persister1 = RocksDbPersister::new(
//!     db.clone(),
//!     "changes".to_owned(),
//!     "documents".to_owned(),
//!     "sync-states".to_owned(),
//!     "1".to_owned(),
//! )?;
//! let backend1 = PersistentBackend::<_, automerge::Backend>::load(persister1)?;
//!
//! let persister2 = RocksDbPersister::new(
//!     db,
//!     "changes".to_owned(),
//!     "documents".to_owned(),
//!     "sync-states".to_owned(),
//!     "2".to_owned(),
//! )?;
//! let backend2 = PersistentBackend::<_, automerge::Backend>::load(persister2)?;
//! # Ok(())
//! # }
//! ```

use std::sync::{Arc, Mutex};

use automerge_persistent::{Persister, StoredSizes};
use automerge_protocol::ActorId;

/// The key to use to store the document in the document tree
const DOCUMENT_KEY: &[u8] = b"document";

/// The persister that stores changes and documents in rocksdb.
///
/// Changes and documents are kept under separate prefixes.
///
/// An optional prefix can be used in case multiple persisters may share the same db.
#[derive(Debug)]
pub struct RocksDbPersister {
    db: Arc<Mutex<rocksdb::DB>>,
    changes_prefix: String,
    document_prefix: String,
    sync_states_prefix: String,
    prefix: String,
    sizes: StoredSizes,
}

/// Possible errors from persisting.
#[derive(Debug, thiserror::Error)]
pub enum RocksDbPersisterError {
    /// Internal errors from rocksdb.
    #[error(transparent)]
    RocksDbError(#[from] rocksdb::Error),
}

impl<B> From<RocksDbPersisterError> for automerge_persistent::Error<RocksDbPersisterError, B>
where
    B: std::error::Error + 'static,
{
    fn from(e: RocksDbPersisterError) -> Self {
        Self::PersisterError(e)
    }
}

impl RocksDbPersister {
    /// Construct a new persister.
    #[must_use]
    pub fn new(
        db: Arc<Mutex<rocksdb::DB>>,
        changes_prefix: String,
        document_prefix: String,
        sync_states_prefix: String,
        prefix: String,
    ) -> Result<Self, RocksDbPersisterError> {
        let mut s = Self {
            db,
            changes_prefix,
            document_prefix,
            sync_states_prefix,
            prefix,
            sizes: StoredSizes::default(),
        };
        s.sizes.changes = s.get_changes()?.iter().map(Vec::len).sum();
        s.sizes.document = s.get_document()?.unwrap_or_default().len();
        s.sizes.sync_states = s
            .get_peer_ids()?
            .iter()
            .map(|id| s.get_sync_state(id).map(|o| o.unwrap_or_default().len()))
            .collect::<Result<Vec<usize>, _>>()?
            .iter()
            .sum();
        Ok(s)
    }

    /// Make a key from the prefix, `actor_id` and `sequence_number`.
    ///
    /// Converts the `actor_id` to bytes and appends the `sequence_number` in big endian form.
    fn make_key(&self, actor_id: &ActorId, seq: u64) -> Vec<u8> {
        let mut key = self.changes_prefix.as_bytes().to_vec();
        key.extend(self.prefix.as_bytes());
        key.extend(actor_id.to_bytes());
        key.extend(&seq.to_be_bytes());
        key
    }

    fn make_document_key(&self) -> Vec<u8> {
        let mut key = self.document_prefix.as_bytes().to_vec();
        key.extend(self.prefix.as_bytes());
        key.extend(DOCUMENT_KEY);
        key
    }

    fn make_peer_key(&self, peer_id: &[u8]) -> Vec<u8> {
        let mut key = self.sync_states_prefix.as_bytes().to_vec();
        key.extend(self.prefix.as_bytes());
        key.extend(peer_id);
        key
    }
}

#[async_trait::async_trait]
impl Persister for RocksDbPersister {
    type Error = RocksDbPersisterError;

    /// Get all of the current changes.
    fn get_changes(&self) -> Result<Vec<Vec<u8>>, Self::Error> {
        let mut prefix = self.changes_prefix.as_bytes().to_vec();
        prefix.extend(self.prefix.as_bytes());
        Ok(self
            .db
            .lock()
            .unwrap()
            .prefix_iterator(prefix)
            .map(|(_, v)| v.to_vec())
            .collect())
    }

    /// Insert all of the given changes into the db.
    fn insert_changes<I>(&mut self, changes: I) -> Result<(), Self::Error>
    where
        I: IntoIterator<Item = (ActorId, u64, Vec<u8>)>,
    {
        let db = self.db.lock().unwrap();
        for (a, s, c) in changes {
            let key = self.make_key(&a, s);
            self.sizes.changes += c.len();
            if let Some(old) = db.get_pinned(&key)? {
                self.sizes.changes -= old.len();
            }
            db.put(key, c)?;
        }
        Ok(())
    }

    /// Remove all of the given changes from the db.
    fn remove_changes<'a, I>(&mut self, changes: I) -> Result<(), Self::Error>
    where
        I: IntoIterator<Item = (&'a ActorId, u64)>,
    {
        let db = self.db.lock().unwrap();
        for (a, s) in changes {
            let key = self.make_key(a, s);
            if let Some(old) = db.get_pinned(&key)? {
                self.sizes.changes -= old.len();
            }
            db.delete(key)?;
        }
        Ok(())
    }

    /// Retrieve the document from the db.
    fn get_document(&self) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.db.lock().unwrap().get(self.make_document_key())?)
    }

    /// Set the document in the db.
    fn set_document(&mut self, data: Vec<u8>) -> Result<(), Self::Error> {
        self.sizes.document = data.len();
        self.db
            .lock()
            .unwrap()
            .put(self.make_document_key(), data)?;
        Ok(())
    }

    fn get_sync_state(&self, peer_id: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let sync_state_key = self.make_peer_key(peer_id);
        Ok(self.db.lock().unwrap().get(sync_state_key)?)
    }

    fn set_sync_state(&mut self, peer_id: Vec<u8>, sync_state: Vec<u8>) -> Result<(), Self::Error> {
        let sync_state_key = self.make_peer_key(&peer_id);
        self.sizes.sync_states += sync_state.len();

        let db = self.db.lock().unwrap();
        if let Some(old) = db.get_pinned(&sync_state_key)? {
            self.sizes.sync_states -= old.len();
        }
        db.put(sync_state_key, sync_state)?;

        Ok(())
    }

    fn remove_sync_states(&mut self, peer_ids: &[&[u8]]) -> Result<(), Self::Error> {
        let db = self.db.lock().unwrap();
        for id in peer_ids {
            let key = self.make_peer_key(id);

            if let Some(old) = db.get_pinned(&key)? {
                self.sizes.sync_states -= old.len();
            }

            db.delete(key)?;
        }
        Ok(())
    }

    fn get_peer_ids(&self) -> Result<Vec<Vec<u8>>, Self::Error> {
        let mut prefix = self.sync_states_prefix.as_bytes().to_vec();
        prefix.extend(self.prefix.as_bytes());

        Ok(self
            .db
            .lock()
            .unwrap()
            .prefix_iterator(prefix)
            .map(|(k, _)| k.to_vec())
            .collect())
    }

    fn sizes(&self) -> StoredSizes {
        self.sizes.clone()
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        self.db.lock().unwrap().flush()?;
        Ok(())
    }
}
