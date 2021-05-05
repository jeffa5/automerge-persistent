#![warn(missing_docs)]
#![warn(missing_crate_level_docs)]
#![warn(missing_doc_code_examples)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

//! A persister targetting `LocalStorage` in the browser.
//!
//! ```rust,no_run
//! # use automerge_persistent_localstorage::{LocalStoragePersister, LocalStoragePersisterError};
//! # use automerge_persistent::PersistentBackend;
//! # fn main() -> Result<(), LocalStoragePersisterError> {
//! let storage = web_sys::window()
//!     .unwrap()
//!     .local_storage()
//!     .map_err(LocalStoragePersisterError::StorageError)?
//!     .unwrap();
//!
//! let persister = LocalStoragePersister::new(storage, "document".to_owned(), "changes".to_owned(), "sync-states".to_owned())?;
//! let backend = PersistentBackend::load(persister);
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;

use automerge_protocol::ActorId;

/// Persist changes and documents in to `LocalStorage`.
///
/// While aimed at `LocalStorage`, it accepts any storage that  conforms to the [`web_sys::Storage`]
/// API.
///
/// Since `LocalStorage` is limited we store changes in a JSON map in one key.
#[derive(Debug)]
pub struct LocalStoragePersister {
    storage: web_sys::Storage,
    changes: HashMap<String, Vec<u8>>,
    sync_states: HashMap<Vec<u8>, Vec<u8>>,
    document_key: String,
    changes_key: String,
    sync_states_key: String,
}

/// Possible errors from persisting.
#[derive(Debug, thiserror::Error)]
pub enum LocalStoragePersisterError {
    /// Serde failure, converting the change/document into JSON.
    #[error(transparent)]
    SerdeError(#[from] serde_json::Error),
    /// An underlying storage error.
    #[error("storage error {0:?}")]
    StorageError(wasm_bindgen::JsValue),
}

impl LocalStoragePersister {
    /// Construct a new `LocalStoragePersister`.
    pub fn new(
        storage: web_sys::Storage,
        document_key: String,
        changes_key: String,
        sync_states_key: String,
    ) -> Result<Self, LocalStoragePersisterError> {
        let changes = if let Some(stored) = storage
            .get_item(&changes_key)
            .map_err(LocalStoragePersisterError::StorageError)?
        {
            serde_json::from_str(&stored)?
        } else {
            HashMap::new()
        };
        let sync_states = if let Some(stored) = storage
            .get_item(&sync_states_key)
            .map_err(LocalStoragePersisterError::StorageError)?
        {
            serde_json::from_str(&stored)?
        } else {
            HashMap::new()
        };
        Ok(Self {
            storage,
            changes,
            sync_states,
            document_key,
            changes_key,
            sync_states_key,
        })
    }
}

impl automerge_persistent::Persister for LocalStoragePersister {
    type Error = LocalStoragePersisterError;

    fn get_changes(&self) -> Result<Vec<Vec<u8>>, Self::Error> {
        Ok(self.changes.values().cloned().collect())
    }

    fn insert_changes(&mut self, changes: Vec<(ActorId, u64, Vec<u8>)>) -> Result<(), Self::Error> {
        for (a, s, c) in changes {
            let key = make_key(&a, s);

            self.changes.insert(key, c);
        }
        self.storage
            .set_item(&self.changes_key, &serde_json::to_string(&self.changes)?)
            .map_err(LocalStoragePersisterError::StorageError)?;
        Ok(())
    }

    fn remove_changes(&mut self, changes: Vec<(&ActorId, u64)>) -> Result<(), Self::Error> {
        let mut some_removal = false;
        for (a, s) in changes {
            let key = make_key(a, s);
            if self.changes.remove(&key).is_some() {
                some_removal = true
            }
        }

        if some_removal {
            let s = serde_json::to_string(&self.changes)?;
            self.storage
                .set_item(&self.changes_key, &s)
                .map_err(LocalStoragePersisterError::StorageError)?;
        }
        Ok(())
    }

    fn get_document(&self) -> Result<Option<Vec<u8>>, Self::Error> {
        if let Some(doc_string) = self
            .storage
            .get_item(&self.document_key)
            .map_err(LocalStoragePersisterError::StorageError)?
        {
            let doc = serde_json::from_str(&doc_string)?;
            Ok(Some(doc))
        } else {
            Ok(None)
        }
    }

    fn set_document(&mut self, data: Vec<u8>) -> Result<(), Self::Error> {
        let data = serde_json::to_string(&data)?;
        self.storage
            .set_item(&self.document_key, &data)
            .map_err(LocalStoragePersisterError::StorageError)?;
        Ok(())
    }

    fn get_sync_state(&self, peer_id: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.sync_states.get(peer_id).cloned())
    }

    fn set_sync_state(&mut self, peer_id: Vec<u8>, sync_state: Vec<u8>) -> Result<(), Self::Error> {
        self.sync_states.insert(peer_id, sync_state);
        self.storage
            .set_item(
                &self.sync_states_key,
                &serde_json::to_string(&self.sync_states)?,
            )
            .map_err(LocalStoragePersisterError::StorageError)?;
        Ok(())
    }

    fn remove_sync_states(&mut self, peer_ids: &[&[u8]]) -> Result<(), Self::Error> {
        for id in peer_ids {
            self.sync_states.remove(*id);
        }
        self.storage
            .set_item(
                &self.sync_states_key,
                &serde_json::to_string(&self.sync_states)?,
            )
            .map_err(LocalStoragePersisterError::StorageError)?;
        Ok(())
    }

    fn get_peer_ids(&self) -> Result<Vec<Vec<u8>>, Self::Error> {
        Ok(self.sync_states.keys().cloned().collect())
    }
}

/// Make a key from the `actor_id` and `sequence_number`.
///
/// Converts the `actor_id` to a string and appends the `sequence_number`.
fn make_key(actor_id: &ActorId, seq: u64) -> String {
    let mut key = actor_id.to_hex_string();
    key.push_str(&seq.to_string());
    key
}
