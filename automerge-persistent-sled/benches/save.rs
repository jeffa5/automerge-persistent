use automerge::{transaction::Transactable, ROOT};
use automerge_persistent::PersistentAutomerge;
use criterion::{criterion_group, criterion_main, Criterion};

fn small_backend_apply_local_change(c: &mut Criterion) {
    c.bench_function("small backend apply local change", |b| {
        b.iter_batched(
            || {
                let db = sled::Config::new().temporary(true).open().unwrap();
                let sled = automerge_persistent_sled::SledPersister::new(
                    db.open_tree("changes").unwrap(),
                    db.open_tree("document").unwrap(),
                    db.open_tree("sync_states").unwrap(),
                    "".to_owned(),
                )
                .unwrap();
                let mut doc: PersistentAutomerge<automerge_persistent_sled::SledPersister> =
                    automerge_persistent::PersistentAutomerge::load(sled).unwrap();
                doc.transact::<_, _, std::convert::Infallible>(|doc| {
                    doc.set(ROOT, "a", "abcdef").unwrap();
                    Ok(())
                })
                .unwrap();
                let change = doc.document().get_last_local_change().cloned();

                (doc, change.unwrap())
            },
            |(mut persistent_doc, change)| {
                persistent_doc.document_mut().apply_changes(vec![change])
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

fn small_backend_apply_local_change_flush(c: &mut Criterion) {
    c.bench_function("small backend apply local change flush", |b| {
        b.iter_batched(
            || {
                let db = sled::Config::new().temporary(true).open().unwrap();
                let sled = automerge_persistent_sled::SledPersister::new(
                    db.open_tree("changes").unwrap(),
                    db.open_tree("document").unwrap(),
                    db.open_tree("sync_states").unwrap(),
                    "".to_owned(),
                )
                .unwrap();
                let mut doc: PersistentAutomerge<automerge_persistent_sled::SledPersister> =
                    automerge_persistent::PersistentAutomerge::load(sled).unwrap();
                doc.transact::<_, _, std::convert::Infallible>(|doc| {
                    doc.set(ROOT, "a", "abcdef").unwrap();
                    Ok(())
                })
                .unwrap();
                let change = doc.document().get_last_local_change().cloned();

                (db, doc, change.unwrap())
            },
            |(db, mut persistent_doc, change)| {
                persistent_doc
                    .document_mut()
                    .apply_changes(vec![change])
                    .unwrap();
                db.flush().unwrap()
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

fn small_backend_apply_changes(c: &mut Criterion) {
    c.bench_function("small backend apply changes", |b| {
        b.iter_batched(
            || {
                let db = sled::Config::new().temporary(true).open().unwrap();
                let sled = automerge_persistent_sled::SledPersister::new(
                    db.open_tree("changes").unwrap(),
                    db.open_tree("document").unwrap(),
                    db.open_tree("sync_states").unwrap(),
                    "".to_owned(),
                )
                .unwrap();
                let other_backend = automerge::Automerge::new();
                let mut doc: PersistentAutomerge<automerge_persistent_sled::SledPersister> =
                    automerge_persistent::PersistentAutomerge::load(sled).unwrap();
                doc.transact::<_, _, std::convert::Infallible>(|doc| {
                    doc.set(ROOT, "a", "abcdef").unwrap();
                    Ok(())
                })
                .unwrap();
                let changes = other_backend
                    .get_changes(&[])
                    .into_iter()
                    .cloned()
                    .collect();
                (doc, changes)
            },
            |(mut persistent_doc, changes)| persistent_doc.document_mut().apply_changes(changes),
            criterion::BatchSize::SmallInput,
        )
    });
}

fn small_backend_compact(c: &mut Criterion) {
    c.bench_function("small backend compact", |b| {
        b.iter_batched(
            || {
                let db = sled::Config::new().temporary(true).open().unwrap();
                let sled = automerge_persistent_sled::SledPersister::new(
                    db.open_tree("changes").unwrap(),
                    db.open_tree("document").unwrap(),
                    db.open_tree("sync_states").unwrap(),
                    "".to_owned(),
                )
                .unwrap();
                let mut doc: PersistentAutomerge<automerge_persistent_sled::SledPersister> =
                    automerge_persistent::PersistentAutomerge::load(sled).unwrap();
                doc.transact::<_, _, std::convert::Infallible>(|doc| {
                    doc.set(ROOT, "a", "abcdef").unwrap();
                    Ok(())
                })
                .unwrap();
                doc
            },
            |mut persistent_doc| persistent_doc.compact(&[]),
            criterion::BatchSize::SmallInput,
        )
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(50);
    targets = small_backend_apply_local_change, small_backend_apply_local_change_flush, small_backend_apply_changes, small_backend_compact
}
criterion_main!(benches);
