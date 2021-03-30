use std::{error::Error, fmt::Debug, rc::Rc};

use automerge::Change;
use automerge_backend::AutomergeError;
use automerge_protocol::{ActorId, ChangeHash, Patch, UncompressedChange};

pub trait Persister {
    type Error: Error;

    fn get_changes(&self) -> Result<Vec<Vec<u8>>, Self::Error>;

    fn insert_change(
        &mut self,
        actor_id: ActorId,
        seq: u64,
        change: Change,
    ) -> Result<(), Self::Error>;

    fn remove_change(&mut self, actor_id: &ActorId, seq: u64) -> Result<(), Self::Error>;

    fn get_document(&self) -> Result<Option<Vec<u8>>, Self::Error>;

    fn set_document(&mut self, data: Vec<u8>) -> Result<(), Self::Error>;
}

#[derive(Debug, thiserror::Error)]
pub enum PersistentBackendError<E>
where
    E: Debug + Error + 'static,
{
    #[error(transparent)]
    AutomergeError(#[from] AutomergeError),
    #[error(transparent)]
    PersisterError(E),
}

pub struct PersistentBackend<P: Persister> {
    backend: automerge::Backend,
    persister: P,
}

impl<P> PersistentBackend<P>
where
    P: Persister,
{
    pub fn new(persister: P) -> Self {
        Self {
            backend: automerge::Backend::init(),
            persister,
        }
    }

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
        Ok(Self { backend, persister })
    }

    pub fn apply_changes(
        &mut self,
        changes: Vec<Change>,
    ) -> Result<Patch, PersistentBackendError<P::Error>> {
        for change in &changes {
            self.persister
                .insert_change(change.actor_id().clone(), change.seq, change.clone())
                .map_err(PersistentBackendError::PersisterError)?;
        }
        self.backend
            .apply_changes(changes)
            .map_err(PersistentBackendError::AutomergeError)
    }

    pub fn get_heads(&self) -> Vec<ChangeHash> {
        self.backend.get_heads()
    }

    pub fn apply_local_change(
        &mut self,
        change: UncompressedChange,
    ) -> Result<(Patch, Rc<Change>), PersistentBackendError<P::Error>> {
        let (patch, change) = self.backend.apply_local_change(change)?;
        self.persister
            .insert_change(change.actor_id().clone(), change.seq, (*change).clone())
            .map_err(PersistentBackendError::PersisterError)?;
        Ok((patch, change))
    }

    pub fn get_patch(&self) -> Result<Patch, PersistentBackendError<P::Error>> {
        self.backend
            .get_patch()
            .map_err(PersistentBackendError::AutomergeError)
    }

    pub fn get_changes_for_actor_id(
        &self,
        actor_id: &ActorId,
    ) -> Result<Vec<&Change>, PersistentBackendError<P::Error>> {
        self.backend
            .get_changes_for_actor_id(actor_id)
            .map_err(PersistentBackendError::AutomergeError)
    }

    pub fn get_changes(&self, have_deps: &[ChangeHash]) -> Vec<&Change> {
        self.backend.get_changes(have_deps)
    }

    pub fn get_missing_deps(&self) -> Vec<ChangeHash> {
        self.backend.get_missing_deps()
    }

    pub fn compact(&mut self) -> Result<(), PersistentBackendError<P::Error>> {
        let changes = self.backend.get_changes(&[]);
        let saved_backend = self.backend.save()?;
        self.persister
            .set_document(saved_backend)
            .map_err(PersistentBackendError::PersisterError)?;
        for change in changes {
            self.persister
                .remove_change(change.actor_id(), change.seq)
                .map_err(PersistentBackendError::PersisterError)?
        }
        Ok(())
    }
}
