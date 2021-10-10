use std::error::Error;

use automerge::Change;
use automerge_backend::{EventHandler, EventHandlerId, SyncMessage, SyncState};
use automerge_protocol::{ActorId, ChangeHash, Patch};

pub trait Backend: Sized + Default {
    type Error: Error;

    fn load(document: Vec<u8>) -> Result<Self, Self::Error>;

    fn add_event_handler(&mut self, event_handler: EventHandler) -> EventHandlerId;

    fn apply_changes(&mut self, changes: Vec<Change>) -> Result<Patch, Self::Error>;

    fn apply_local_change(
        &mut self,
        change: automerge_protocol::Change,
    ) -> Result<(Patch, &Change), Self::Error>;

    fn get_changes(&self, have_deps: &[ChangeHash]) -> Vec<&Change>;

    fn save(&self) -> Result<Vec<u8>, Self::Error>;

    fn get_patch(&self) -> Result<Patch, Self::Error>;

    fn get_changes_for_actor_id(&self, actor_id: &ActorId) -> Result<Vec<&Change>, Self::Error>;

    fn get_missing_deps(&self, heads: &[ChangeHash]) -> Vec<ChangeHash>;

    fn get_heads(&self) -> Vec<ChangeHash>;

    fn generate_sync_message(
        &self,
        sync_state: &mut SyncState,
    ) -> Result<Option<SyncMessage>, Self::Error>;

    fn receive_sync_message(
        &mut self,
        sync_state: &mut SyncState,
        message: SyncMessage,
    ) -> Result<Option<Patch>, Self::Error>;
}

impl Backend for automerge::Backend {
    type Error = automerge_backend::AutomergeError;

    fn load(document: Vec<u8>) -> Result<Self, Self::Error> {
        Self::load(document)
    }

    fn add_event_handler(&mut self, event_handler: EventHandler) -> EventHandlerId {
        self.add_event_handler(event_handler)
    }

    fn apply_changes(&mut self, changes: Vec<Change>) -> Result<Patch, Self::Error> {
        self.apply_changes(changes)
    }

    fn apply_local_change(
        &mut self,
        change: automerge_protocol::Change,
    ) -> Result<(Patch, &Change), Self::Error> {
        self.apply_local_change(change)
    }

    fn get_changes(&self, have_deps: &[ChangeHash]) -> Vec<&Change> {
        self.get_changes(have_deps)
    }

    fn save(&self) -> Result<Vec<u8>, Self::Error> {
        self.save()
    }

    fn get_patch(&self) -> Result<Patch, Self::Error> {
        self.get_patch()
    }

    fn get_changes_for_actor_id(&self, actor_id: &ActorId) -> Result<Vec<&Change>, Self::Error> {
        self.get_changes_for_actor_id(actor_id)
    }

    fn get_missing_deps(&self, heads: &[ChangeHash]) -> Vec<ChangeHash> {
        self.get_missing_deps(heads)
    }

    fn get_heads(&self) -> Vec<ChangeHash> {
        self.get_heads()
    }

    fn generate_sync_message(
        &self,
        sync_state: &mut SyncState,
    ) -> Result<Option<SyncMessage>, Self::Error> {
        Ok(Self::generate_sync_message(self, sync_state))
    }

    fn receive_sync_message(
        &mut self,
        sync_state: &mut SyncState,
        message: SyncMessage,
    ) -> Result<Option<Patch>, Self::Error> {
        self.receive_sync_message(sync_state, message)
    }
}
