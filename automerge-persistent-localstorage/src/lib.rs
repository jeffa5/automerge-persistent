use std::collections::HashMap;

use automerge_protocol::ActorId;

#[derive(Debug)]
pub struct LocalStoragePersister {
    storage: web_sys::Storage,
    changes: HashMap<String, Vec<u8>>,
    document_key: String,
    changes_key: String,
}

#[derive(Debug, thiserror::Error)]
pub enum LocalStoragePersisterError {
    #[error(transparent)]
    SerdeError(#[from] serde_json::Error),
    #[error("storage error {0:?}")]
    StorageError(wasm_bindgen::JsValue),
}

impl LocalStoragePersister {
    pub fn new(
        storage: web_sys::Storage,
        document_key: String,
        changes_key: String,
    ) -> Result<Self, LocalStoragePersisterError> {
        let changes = serde_json::from_str(
            &storage
                .get_item(&changes_key)
                .map_err(LocalStoragePersisterError::StorageError)?
                .unwrap_or_else(|| "{}".to_owned()),
        )?;
        Ok(Self {
            storage,
            changes,
            document_key,
            changes_key,
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
}

/// Make a key from the actor_id and sequence_number.
///
/// Converts the actor_id to a string and appends the sequence_number.
fn make_key(actor_id: &ActorId, seq: u64) -> String {
    let mut key = actor_id.to_hex_string();
    key.push_str(&seq.to_string());
    key
}
