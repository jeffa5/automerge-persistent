use std::collections::HashMap;

use automerge_protocol::ActorId;

use crate::Persister;

/// **For Testing** An in-memory persister.
///
/// As this provides no actual persistence it should not be used for any real application, it
/// actually reduces performance of the plain backend slightly due to tracking the changes itself.
#[derive(Debug, Default)]
pub struct MemoryPersister {
    changes: HashMap<(ActorId, u64), Vec<u8>>,
    document: Option<Vec<u8>>,
}

impl Persister for MemoryPersister {
    type Error = std::convert::Infallible;

    /// Get the changes out of the map.
    fn get_changes(&self) -> Result<Vec<Vec<u8>>, Self::Error> {
        Ok(self.changes.values().cloned().collect())
    }

    /// Insert changes into the map.
    fn insert_changes(&mut self, changes: Vec<(ActorId, u64, Vec<u8>)>) -> Result<(), Self::Error> {
        for (a, u, c) in changes {
            self.changes.insert((a, u), c);
        }
        Ok(())
    }

    /// Remove changes from the map.
    fn remove_changes(&mut self, changes: Vec<(&ActorId, u64)>) -> Result<(), Self::Error> {
        for (a, u) in changes {
            self.changes.remove(&(a.clone(), u));
        }
        Ok(())
    }

    /// Get the document.
    fn get_document(&self) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.document.clone())
    }

    /// Set the document.
    fn set_document(&mut self, data: Vec<u8>) -> Result<(), Self::Error> {
        self.document = Some(data);
        Ok(())
    }
}
