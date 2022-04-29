use std::{
    collections::HashMap,
    fs,
    os::unix::prelude::OsStrExt,
    path::{Path, PathBuf},
};

use automerge::ActorId;
use automerge_persistent::{Persister, StoredSizes};
use futures::{Future, FutureExt, TryStreamExt};
use hex::FromHexError;

#[derive(Debug)]
pub struct FsPersister {
    changes_path: PathBuf,
    doc_path: PathBuf,
    sync_states_path: PathBuf,
    cache: FsPersisterCache,
    sizes: StoredSizes,
}

#[derive(Debug)]
pub struct FsPersisterCache {
    changes: HashMap<(ActorId, u64), Vec<u8>>,
    document: Option<Vec<u8>>,
    sync_states: HashMap<Vec<u8>, Vec<u8>>,
}

impl FsPersisterCache {
    fn flush_changes_sync(&mut self, changes_path: PathBuf) -> Result<usize, std::io::Error> {
        let mut flushed = 0;
        for ((a, s), c) in self.changes.drain() {
            fs::write(make_changes_path(&changes_path, &a, s), &c)?;
            flushed += c.len();
        }
        Ok(flushed)
    }

    fn flush_document_sync(&mut self, doc_path: PathBuf) -> Result<usize, std::io::Error> {
        let mut flushed = 0;
        if let Some(data) = self.document.take() {
            fs::write(&doc_path, &data)?;
            flushed = data.len();
        }
        Ok(flushed)
    }

    fn flush_sync_states_sync(
        &mut self,
        sync_states_path: PathBuf,
    ) -> Result<usize, std::io::Error> {
        let mut flushed = 0;
        for (peer_id, sync_state) in self.sync_states.drain() {
            fs::write(make_peer_path(&sync_states_path, &peer_id), &sync_state)?;
            flushed += sync_state.len();
        }
        Ok(flushed)
    }

    async fn flush_changes(&mut self, changes_path: PathBuf) -> Result<usize, std::io::Error> {
        let futs = futures::stream::FuturesUnordered::new();
        for ((a, s), c) in self.changes.drain() {
            let len = c.len();
            futs.push(
                tokio::fs::write(make_changes_path(&changes_path, &a, s), c).map(move |_| Ok(len)),
            );
        }
        let res: Result<Vec<usize>, std::io::Error> = futs.try_collect().await;
        Ok(res?.iter().sum())
    }

    async fn flush_document(&mut self, doc_path: PathBuf) -> Result<usize, std::io::Error> {
        let mut flushed = 0;
        if let Some(data) = self.document.take() {
            tokio::fs::write(&doc_path, &data).await?;
            flushed = data.len();
        }
        Ok(flushed)
    }

    async fn flush_sync_states(
        &mut self,
        sync_states_path: PathBuf,
    ) -> Result<usize, std::io::Error> {
        let futs = futures::stream::FuturesUnordered::new();
        for (peer_id, sync_state) in self.sync_states.drain() {
            let len = sync_state.len();
            futs.push(
                tokio::fs::write(make_peer_path(&sync_states_path, &peer_id), sync_state)
                    .map(move |_| Ok(len)),
            );
        }
        let res: Result<Vec<usize>, std::io::Error> = futs.try_collect().await;
        Ok(res?.iter().sum())
    }

    pub async fn flush(
        &mut self,
        doc_path: PathBuf,
        changes_path: PathBuf,
        sync_states_path: PathBuf,
    ) -> Result<usize, std::io::Error> {
        let mut flushed = 0;
        flushed += self.flush_document(doc_path).await?;
        flushed += self.flush_changes(changes_path).await?;
        flushed += self.flush_sync_states(sync_states_path).await?;
        Ok(flushed)
    }

    pub fn flush_sync(
        &mut self,
        doc_path: PathBuf,
        changes_path: PathBuf,
        sync_states_path: PathBuf,
    ) -> Result<usize, std::io::Error> {
        let mut flushed = 0;
        flushed += self.flush_document_sync(doc_path)?;
        flushed += self.flush_changes_sync(changes_path)?;
        flushed += self.flush_sync_states_sync(sync_states_path)?;
        Ok(flushed)
    }

    fn drain_clone(&mut self) -> Self {
        Self {
            changes: self.changes.drain().collect(),
            document: self.document.take(),
            sync_states: self.sync_states.drain().collect(),
        }
    }
}

/// Possible errors from persisting.
#[derive(Debug, thiserror::Error)]
pub enum FsPersisterError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Hex(#[from] FromHexError),
}

const CHANGES_DIR: &str = "changes";
const DOC_FILE: &str = "doc";
const SYNC_DIR: &str = "sync";

impl FsPersister {
    pub fn new<R: AsRef<Path>, P: AsRef<Path>>(
        root: R,
        prefix: P,
    ) -> Result<Self, FsPersisterError> {
        let root_path = root.as_ref().join(&prefix);
        fs::create_dir_all(&root_path)?;

        let changes_path = root_path.join(CHANGES_DIR);
        if fs::metadata(&changes_path).is_err() {
            fs::create_dir(&changes_path)?;
        }

        let doc_path = root_path.join(DOC_FILE);

        let sync_states_path = root_path.join(SYNC_DIR);
        if fs::metadata(&sync_states_path).is_err() {
            fs::create_dir(&sync_states_path)?;
        }

        let mut s = Self {
            changes_path,
            doc_path,
            sync_states_path,
            cache: FsPersisterCache {
                changes: HashMap::new(),
                document: None,
                sync_states: HashMap::new(),
            },
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

    pub fn flush_cache(&mut self) -> impl Future<Output = Result<usize, std::io::Error>> {
        let doc_path = self.doc_path.clone();
        let changes_path = self.changes_path.clone();
        let sync_states_path = self.sync_states_path.clone();
        let mut cache = self.cache.drain_clone();
        async move { cache.flush(doc_path, changes_path, sync_states_path).await }
    }

    pub fn load<R: AsRef<Path>, P: AsRef<Path>>(
        root: R,
        prefix: P,
    ) -> Result<Option<Self>, FsPersisterError> {
        if !root.as_ref().join(&prefix).exists() {
            return Ok(None);
        }
        let doc = Self::new(root, prefix)?;
        Ok(Some(doc))
    }
}

fn make_changes_path<P: AsRef<Path>>(changes_path: P, actor_id: &ActorId, seq: u64) -> PathBuf {
    changes_path
        .as_ref()
        .join(format!("{}-{}", actor_id.to_hex_string(), seq))
}

fn make_peer_path<P: AsRef<Path>>(sync_states_path: P, peer_id: &[u8]) -> PathBuf {
    sync_states_path.as_ref().join(hex::encode(peer_id))
}

impl Persister for FsPersister {
    type Error = FsPersisterError;

    fn get_changes(&self) -> Result<Vec<Vec<u8>>, Self::Error> {
        fs::read_dir(&self.changes_path)?
            .filter_map(|entry| {
                if let Ok((Ok(file_type), path)) =
                    entry.map(|entry| (entry.file_type(), entry.path()))
                {
                    if file_type.is_file() {
                        Some(fs::read(path).map_err(FsPersisterError::from))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect()
    }

    fn insert_changes(&mut self, changes: Vec<(ActorId, u64, Vec<u8>)>) -> Result<(), Self::Error> {
        for (a, s, c) in changes {
            self.sizes.changes += c.len();
            if let Some(old) = self.cache.changes.insert((a, s), c) {
                self.sizes.changes -= old.len();
            }
        }
        Ok(())
    }

    fn remove_changes(&mut self, changes: Vec<(&ActorId, u64)>) -> Result<(), Self::Error> {
        for (a, s) in changes {
            if let Some(old) = self.cache.changes.remove(&(a.clone(), s)) {
                // not flushed yet
                self.sizes.changes -= old.len();
                continue;
            }

            let path = make_changes_path(&self.changes_path, a, s);
            if let Ok(meta) = fs::metadata(&path) {
                if meta.is_file() {
                    fs::remove_file(&path)?;
                    self.sizes.changes -= meta.len() as usize;
                }
            }
        }
        Ok(())
    }

    fn get_document(&self) -> Result<Option<Vec<u8>>, Self::Error> {
        if let Some(ref doc) = self.cache.document {
            return Ok(Some(doc.clone()));
        }
        if fs::metadata(&self.doc_path).is_ok() {
            return Ok(fs::read(&self.doc_path).map(|v| if v.is_empty() { None } else { Some(v) })?);
        }
        Ok(None)
    }

    fn set_document(&mut self, data: Vec<u8>) -> Result<(), Self::Error> {
        self.sizes.document = data.len();
        self.cache.document = Some(data);
        Ok(())
    }

    fn get_sync_state(&self, peer_id: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        if let Some(sync_state) = self.cache.sync_states.get(peer_id) {
            return Ok(Some(sync_state.clone()));
        }
        let path = make_peer_path(&self.sync_states_path, peer_id);
        if fs::metadata(&path).is_ok() {
            return Ok(fs::read(&path).map(|v| if v.is_empty() { None } else { Some(v) })?);
        }
        Ok(None)
    }

    fn set_sync_state(&mut self, peer_id: Vec<u8>, sync_state: Vec<u8>) -> Result<(), Self::Error> {
        self.sizes.sync_states += sync_state.len();
        if let Some(old) = self.cache.sync_states.insert(peer_id, sync_state) {
            self.sizes.sync_states -= old.len();
        }
        Ok(())
    }

    fn remove_sync_states(&mut self, peer_ids: &[&[u8]]) -> Result<(), Self::Error> {
        for peer_id in peer_ids {
            if let Some(old) = self.cache.sync_states.remove(*peer_id) {
                // not flushed yet
                self.sizes.sync_states -= old.len();
                continue;
            }
            let path = make_peer_path(&self.sync_states_path, peer_id);
            if let Ok(meta) = fs::metadata(&path) {
                if meta.is_file() {
                    fs::remove_file(&path)?;
                    self.sizes.sync_states -= meta.len() as usize;
                }
            }
        }
        Ok(())
    }

    fn get_peer_ids(&self) -> Result<Vec<Vec<u8>>, Self::Error> {
        fs::read_dir(&self.sync_states_path)?
            .filter_map(|entry| {
                if let Ok((Ok(file_type), path)) =
                    entry.map(|entry| (entry.file_type(), entry.path()))
                {
                    if file_type.is_file() {
                        Some(
                            hex::decode(path.file_name().unwrap().as_bytes())
                                .map_err(FsPersisterError::from),
                        )
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect()
    }

    fn sizes(&self) -> StoredSizes {
        self.sizes.clone()
    }

    fn flush(&mut self) -> Result<usize, Self::Error> {
        self.cache
            .drain_clone()
            .flush_sync(
                self.doc_path.clone(),
                self.changes_path.clone(),
                self.sync_states_path.clone(),
            )
            .map_err(FsPersisterError::from)
    }
}
