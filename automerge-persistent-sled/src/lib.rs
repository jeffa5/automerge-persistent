#![warn(missing_docs)]
#![warn(missing_crate_level_docs)]
#![warn(missing_doc_code_examples)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

//! A persister targetting [Sled](https://github.com/spacejam/sled).
//!
//! # Single persister
//!
//! ```rust
//! # use automerge_persistent::PersistentBackend;
//! # use automerge_persistent_sled::SledPersister;
//! # use automerge_persistent_sled::SledPersisterError;
//! # fn main() -> Result<(), automerge_persistent::Error<SledPersisterError,
//! automerge_backend::AutomergeError>> {
//! let db = sled::Config::new().temporary(true).open().map_err(SledPersisterError::SledError)?;
//! let changes_tree = db.open_tree("changes").map_err(SledPersisterError::SledError)?;
//! let documents_tree = db.open_tree("documents").map_err(SledPersisterError::SledError)?;
//! let sync_states_tree = db.open_tree("sync-states").map_err(SledPersisterError::SledError)?;
//!
//! let persister = SledPersister::new(
//!     changes_tree,
//!     documents_tree,
//!     sync_states_tree,
//!     String::new(),
//! )?;
//! let backend = PersistentBackend::<_, automerge::Backend>::load(persister)?;
//! # Ok(())
//! # }
//! ```
//!
//! # Multiple persisters sharing the same trees
//!
//! ```rust
//! # use automerge_persistent::PersistentBackend;
//! # use automerge_persistent_sled::SledPersister;
//! # use automerge_persistent_sled::SledPersisterError;
//! # fn main() -> Result<(), automerge_persistent::Error<SledPersisterError,
//! automerge_backend::AutomergeError>> {
//! let db = sled::Config::new().temporary(true).open().map_err(SledPersisterError::SledError)?;
//! let changes_tree = db.open_tree("changes").map_err(SledPersisterError::SledError)?;
//! let documents_tree = db.open_tree("documents").map_err(SledPersisterError::SledError)?;
//! let sync_states_tree = db.open_tree("sync-states").map_err(SledPersisterError::SledError)?;
//!
//! let persister1 = SledPersister::new(
//!     changes_tree.clone(),
//!     documents_tree.clone(),
//!     sync_states_tree.clone(),
//!     "1".to_owned(),
//! )?;
//! let backend1 = PersistentBackend::<_, automerge::Backend>::load(persister1)?;
//!
//! let persister2 = SledPersister::new(
//!     changes_tree,
//!     documents_tree,
//!     sync_states_tree,
//!     "2".to_owned(),
//! )?;
//! let backend2 = PersistentBackend::<_, automerge::Backend>::load(persister2)?;
//! # Ok(())
//! # }
//! ```

use automerge_persistent::{Persister, StoredSizes};
use automerge_protocol::ActorId;

/// The key to use to store the document in the document tree
const DOCUMENT_KEY: &[u8] = b"document";

/// The persister that stores changes and documents in sled trees.
///
/// Changes and documents are kept in separate trees.
///
/// An optional prefix can be used in case multiple persisters may share the same trees.
#[derive(Debug)]
pub struct SledPersister {
    changes_tree: sled::Tree,
    document_tree: sled::Tree,
    sync_states_tree: sled::Tree,
    prefix: String,
    sizes: StoredSizes,
}

/// Possible errors from persisting.
#[derive(Debug, thiserror::Error)]
pub enum SledPersisterError {
    /// Internal errors from sled.
    #[error(transparent)]
    SledError(#[from] sled::Error),
}

impl<B> From<SledPersisterError> for automerge_persistent::Error<SledPersisterError, B>
where
    B: std::error::Error + 'static,
{
    fn from(e: SledPersisterError) -> Self {
        Self::PersisterError(e)
    }
}

impl SledPersister {
    /// Construct a new persister.
    #[must_use]
    pub fn new(
        changes_tree: sled::Tree,
        document_tree: sled::Tree,
        sync_states_tree: sled::Tree,
        prefix: String,
    ) -> Result<Self, SledPersisterError> {
        let mut s = Self {
            changes_tree,
            document_tree,
            sync_states_tree,
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
        let mut key = self.prefix.as_bytes().to_vec();
        key.extend(actor_id.to_bytes());
        key.extend(&seq.to_be_bytes());
        key
    }

    fn make_document_key(&self) -> Vec<u8> {
        let mut key = self.prefix.as_bytes().to_vec();
        key.extend(DOCUMENT_KEY);
        key
    }

    fn make_peer_key(&self, peer_id: &[u8]) -> Vec<u8> {
        let mut key = self.prefix.as_bytes().to_vec();
        key.extend(peer_id);
        key
    }
}

#[async_trait::async_trait]
impl Persister for SledPersister {
    type Error = SledPersisterError;

    /// Get all of the current changes.
    fn get_changes(&self) -> Result<Vec<Vec<u8>>, Self::Error> {
        self.changes_tree
            .scan_prefix(&self.prefix)
            .values()
            .map(|v| v.map(|v| v.to_vec()).map_err(Self::Error::SledError))
            .collect()
    }

    /// Insert all of the given changes into the tree.
    fn insert_changes<I>(&mut self, changes: I) -> Result<(), Self::Error>
    where
        I: IntoIterator<Item = (ActorId, u64, Vec<u8>)>,
    {
        for (a, s, c) in changes {
            let key = self.make_key(&a, s);
            self.sizes.changes += c.len();
            if let Some(old) = self.changes_tree.insert(key, c)? {
                self.sizes.changes -= old.len();
            }
        }
        Ok(())
    }

    /// Remove all of the given changes from the tree.
    fn remove_changes<'a, I>(&mut self, changes: I) -> Result<(), Self::Error>
    where
        I: IntoIterator<Item = (&'a ActorId, u64)>,
    {
        for (a, s) in changes {
            let key = self.make_key(a, s);
            if let Some(old) = self.changes_tree.remove(key)? {
                self.sizes.changes -= old.len();
            }
        }
        Ok(())
    }

    /// Retrieve the document from the tree.
    fn get_document(&self) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self
            .document_tree
            .get(self.make_document_key())?
            .map(|v| v.to_vec()))
    }

    /// Set the document in the tree.
    fn set_document(&mut self, data: Vec<u8>) -> Result<(), Self::Error> {
        self.sizes.document = data.len();
        self.document_tree.insert(self.make_document_key(), data)?;
        Ok(())
    }

    fn get_sync_state(&self, peer_id: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let sync_state_key = self.make_peer_key(peer_id);
        Ok(self
            .sync_states_tree
            .get(sync_state_key)?
            .map(|v| v.to_vec()))
    }

    fn set_sync_state(&mut self, peer_id: Vec<u8>, sync_state: Vec<u8>) -> Result<(), Self::Error> {
        let sync_state_key = self.make_peer_key(&peer_id);
        self.sizes.sync_states += sync_state.len();
        if let Some(old) = self.sync_states_tree.insert(sync_state_key, sync_state)? {
            self.sizes.sync_states -= old.len();
        }
        Ok(())
    }

    fn remove_sync_states(&mut self, peer_ids: &[&[u8]]) -> Result<(), Self::Error> {
        for id in peer_ids {
            let key = self.make_peer_key(id);
            if let Some(old) = self.sync_states_tree.remove(key)? {
                self.sizes.sync_states -= old.len();
            }
        }
        Ok(())
    }

    fn get_peer_ids(&self) -> Result<Vec<Vec<u8>>, Self::Error> {
        self.sync_states_tree
            .scan_prefix(&self.prefix)
            .keys()
            .map(|v| v.map(|v| v.to_vec()).map_err(Self::Error::SledError))
            .collect()
    }

    fn sizes(&self) -> StoredSizes {
        self.sizes.clone()
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        self.changes_tree.flush()?;
        self.document_tree.flush()?;
        self.sync_states_tree.flush()?;
        Ok(())
    }
}
