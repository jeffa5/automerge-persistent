use automerge_protocol::ActorId;

/// The key to use to store the document in the document tree
const DOCUMENT_KEY: &[u8] = b"document";

/// The persister that stores changes and documents in sled trees.
#[derive(Debug)]
pub struct SledPersister {
    // TODO: should we just store a single tree and use a changes/ prefix
    changes_tree: sled::Tree,
    document_tree: sled::Tree,
    prefix: String,
}

impl SledPersister {
    pub fn new(changes_tree: sled::Tree, document_tree: sled::Tree, prefix: String) -> Self {
        Self {
            changes_tree,
            document_tree,
            prefix,
        }
    }
}

impl automerge_persistent::Persister for SledPersister {
    type Error = sled::Error;

    fn get_changes(&self) -> Result<Vec<Vec<u8>>, Self::Error> {
        self.changes_tree
            .iter()
            .values()
            .map(|v| v.map(|v| v.to_vec()))
            .collect()
    }

    fn insert_changes(&mut self, changes: Vec<(ActorId, u64, Vec<u8>)>) -> Result<(), Self::Error> {
        for (a, s, c) in changes {
            let key = self.make_key(&a, s);
            self.changes_tree.insert(key, c)?;
        }
        Ok(())
    }

    fn remove_changes(&mut self, changes: Vec<(&ActorId, u64)>) -> Result<(), Self::Error> {
        for (a, s) in changes {
            let key = self.make_key(a, s);
            self.changes_tree.remove(key)?;
        }
        Ok(())
    }

    fn get_document(&self) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.document_tree.get(DOCUMENT_KEY)?.map(|v| v.to_vec()))
    }

    fn set_document(&mut self, data: Vec<u8>) -> Result<(), Self::Error> {
        self.document_tree.insert(DOCUMENT_KEY, data)?;
        Ok(())
    }
}
impl SledPersister {
    /// Make a key from the actor_id and sequence_number.
    ///
    /// Converts the actor_id to bytes and appends the sequence_number in big endian form.
    fn make_key(&self, actor_id: &ActorId, seq: u64) -> Vec<u8> {
        let mut key = self.prefix.as_bytes().to_vec();
        key.extend(&actor_id.to_bytes());
        key.extend(&seq.to_be_bytes());
        key
    }
}
