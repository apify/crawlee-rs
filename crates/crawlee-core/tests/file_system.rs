//! The file-system backend through the storage frontends: files land where Crawlee for JS and
//! Python put them, and survive a restart.

#![cfg(feature = "fs-storage")]

use std::path::Path;

use crawlee_core::storage::backend::StorageKind;
use crawlee_core::{
    Dataset, FileSystemStorageBackend, KeyValueStore, RequestManager, RequestQueue, StorageBackend, StorageIdentifier,
};
use serde_json::{Value, json};

fn read_json(path: impl AsRef<Path>) -> Value {
    let path = path.as_ref();
    let text = std::fs::read_to_string(path).unwrap_or_else(|err| panic!("{}: {err}", path.display()));
    serde_json::from_str(&text).unwrap()
}

#[tokio::test(flavor = "local")]
async fn dataset_items_are_numbered_files() {
    let dir = tempfile::tempdir().unwrap();
    let backend = FileSystemStorageBackend::new(dir.path());
    let dataset = Dataset::open(&backend, &StorageIdentifier::Default).await.unwrap();

    dataset.push_data(&json!([{ "title": "a", "z": 1, "a": 2 }, { "title": "b" }])).await.unwrap();

    let default = dir.path().join("datasets/default");
    // Key order is kept as pushed.
    assert_eq!(
        std::fs::read_to_string(default.join("000000001.json")).unwrap(),
        "{\n  \"title\": \"a\",\n  \"z\": 1,\n  \"a\": 2\n}"
    );
    assert_eq!(read_json(default.join("000000002.json")), json!({ "title": "b" }));
    assert_eq!(read_json(default.join("__metadata__.json"))["itemCount"], 2);

    let items: Vec<Value> = dataset.get_all().await.unwrap();
    assert_eq!(items, vec![json!({ "title": "a", "z": 1, "a": 2 }), json!({ "title": "b" })]);
    assert_eq!(dataset.get_info().await.unwrap().item_count, 2);
}

#[tokio::test(flavor = "local")]
async fn key_value_store_records_and_adopted_files() {
    let dir = tempfile::tempdir().unwrap();
    // A file put there by hand, without a metadata sidecar.
    let default = dir.path().join("key_value_stores/default");
    std::fs::create_dir_all(&default).unwrap();
    std::fs::write(default.join("INPUT.json"), r#"{"start":"https://crawlee.dev"}"#).unwrap();

    let backend = FileSystemStorageBackend::new(dir.path());
    let store = KeyValueStore::open(&backend, &StorageIdentifier::Default).await.unwrap();

    assert_eq!(store.get_value::<Value>("INPUT.json").await.unwrap(), Some(json!({ "start": "https://crawlee.dev" })));

    store.set_value("OUTPUT", &json!({ "ok": true })).await.unwrap();
    store.set_text("notes", "hello").await.unwrap();
    // The value file is named after the key, with a metadata sidecar next to it.
    assert_eq!(read_json(default.join("OUTPUT")), json!({ "ok": true }));
    assert_eq!(read_json(default.join("OUTPUT.__metadata__.json"))["contentType"], "application/json; charset=utf-8");
    assert_eq!(store.get_text("notes").await.unwrap().as_deref(), Some("hello"));

    let keys: Vec<String> = store.keys(None).await.unwrap().into_iter().map(|item| item.key).collect();
    assert_eq!(keys, ["INPUT.json", "OUTPUT", "notes"]);

    store.delete_value("notes").await.unwrap();
    assert!(!store.record_exists("notes").await.unwrap());
    assert!(store.get_public_url("OUTPUT").await.unwrap().unwrap().starts_with("file://"));
}

#[tokio::test(flavor = "local")]
async fn request_queue_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let backend = FileSystemStorageBackend::new(dir.path());
        let queue = RequestQueue::open(&backend, &StorageIdentifier::Default).await.unwrap();
        queue.add_requests(vec!["https://a.test/1".into(), "https://a.test/2".into()], false).await.unwrap();

        let mut first = queue.fetch_next_request().await.unwrap().unwrap();
        assert_eq!(first.url, "https://a.test/1");
        queue.mark_request_as_handled(&mut first).await.unwrap();
        // The second request is fetched but never handled, as if the process crashed.
        assert_eq!(queue.fetch_next_request().await.unwrap().unwrap().url, "https://a.test/2");
        backend.teardown().await.unwrap();
    }

    let backend = FileSystemStorageBackend::new(dir.path());
    let queue = RequestQueue::open(&backend, &StorageIdentifier::Default).await.unwrap();
    let info = queue.get_info().await.unwrap();
    assert_eq!((info.total_request_count, info.handled_request_count), (2, 1));

    // A handled request is not added again, and the interrupted one is fetched again.
    let result = queue.add_requests(vec!["https://a.test/1".into()], false).await.unwrap();
    assert!(result.processed_requests[0].was_already_handled);
    let mut request = queue.fetch_next_request().await.unwrap().unwrap();
    assert_eq!(request.url, "https://a.test/2");
    queue.mark_request_as_handled(&mut request).await.unwrap();
    assert!(queue.is_finished().await.unwrap());

    // One JSON file per request, next to the queue metadata.
    let stored: Vec<Value> = std::fs::read_dir(dir.path().join("request_queues/default"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| !path.ends_with("__metadata__.json"))
        .map(read_json)
        .collect();
    assert_eq!(stored.len(), 2);
    let second = stored.iter().find(|r| r["url"] == "https://a.test/2").unwrap();
    assert_eq!(second["id"].as_str(), request.id.as_deref());
    assert!(second["handledAt"].is_string());
}

#[tokio::test(flavor = "local")]
async fn purge_empties_run_scoped_storages_only() {
    let dir = tempfile::tempdir().unwrap();
    {
        let backend = FileSystemStorageBackend::new(dir.path());
        for id in [StorageIdentifier::Default, StorageIdentifier::alias("scratch"), StorageIdentifier::name("kept")] {
            Dataset::open(&backend, &id).await.unwrap().push_data(&json!({ "a": 1 })).await.unwrap();
        }
    }

    // A new process: the storages from the previous run are purged without being opened first.
    let backend = FileSystemStorageBackend::new(dir.path());
    backend.purge().await.unwrap();

    let count = |id: StorageIdentifier| {
        let backend = &backend;
        async move { Dataset::open(backend, &id).await.unwrap().get_info().await.unwrap().item_count }
    };
    assert_eq!(count(StorageIdentifier::Default).await, 0);
    assert_eq!(count(StorageIdentifier::alias("scratch")).await, 0);
    assert_eq!(count(StorageIdentifier::name("kept")).await, 1);
    assert!(!dir.path().join("datasets/default/000000001.json").exists());
    assert!(dir.path().join("datasets/kept/000000001.json").exists());
}

#[tokio::test(flavor = "local")]
async fn input_keys_are_adopted_and_kept_on_purge() {
    let dir = tempfile::tempdir().unwrap();
    let default = dir.path().join("key_value_stores/default");
    std::fs::create_dir_all(&default).unwrap();
    std::fs::write(default.join("INPUT.json"), r#"{"start":"https://crawlee.dev"}"#).unwrap();
    std::fs::write(default.join("CUSTOM_INPUT"), "raw").unwrap();
    std::fs::write(default.join("OTHER.json"), "{}").unwrap();

    let backend = FileSystemStorageBackend::new(dir.path()).with_input_keys(["INPUT", "CUSTOM_INPUT"]);
    backend.purge().await.unwrap();
    let store = KeyValueStore::open(&backend, &StorageIdentifier::Default).await.unwrap();

    // `INPUT.json` is the record `INPUT`, not `INPUT.json`, and a bare file keeps its bytes.
    assert_eq!(store.get_value::<Value>("INPUT").await.unwrap(), Some(json!({ "start": "https://crawlee.dev" })));
    let custom = store.get_record("CUSTOM_INPUT").await.unwrap().unwrap();
    assert_eq!((&custom.value[..], custom.content_type.as_deref()), (&b"raw"[..], Some("application/octet-stream")));
    let keys: Vec<String> = store.keys(None).await.unwrap().into_iter().map(|item| item.key).collect();
    assert_eq!(keys, ["CUSTOM_INPUT", "INPUT"], "everything else was purged");

    // Only the default store treats them as input.
    let other = KeyValueStore::open(&backend, &StorageIdentifier::alias("other")).await.unwrap();
    other.set_value("INPUT", &json!(1)).await.unwrap();
    backend.purge().await.unwrap();
    assert!(!other.record_exists("INPUT").await.unwrap());
    assert!(store.record_exists("INPUT").await.unwrap());
}

#[tokio::test(flavor = "local")]
async fn storages_are_cached_and_found_by_id() {
    let dir = tempfile::tempdir().unwrap();
    let backend = FileSystemStorageBackend::new(dir.path());
    let named = Dataset::open(&backend, &StorageIdentifier::name("Products")).await.unwrap();
    let again = Dataset::open(&backend, &StorageIdentifier::name("products")).await.unwrap();
    assert!(named.same_storage(&again), "names are case-insensitive");

    let id = named.get_info().await.unwrap().id;
    assert!(named.same_storage(&Dataset::open(&backend, &StorageIdentifier::id(&id)).await.unwrap()));
    assert!(backend.storage_exists(&id, StorageKind::Dataset).await.unwrap());
    // A name is not an id.
    assert!(!backend.storage_exists("Products", StorageKind::Dataset).await.unwrap());

    // A new backend finds the storage on disk by its id.
    let fresh = FileSystemStorageBackend::new(dir.path());
    assert!(fresh.storage_exists(&id, StorageKind::Dataset).await.unwrap());
    let reopened = Dataset::open(&fresh, &StorageIdentifier::id(&id)).await.unwrap();
    assert_eq!(reopened.get_info().await.unwrap().name.as_deref(), Some("Products"));

    named.drop_storage().await.unwrap();
    assert!(!dir.path().join("datasets/Products").exists());
    let recreated = Dataset::open(&backend, &StorageIdentifier::name("Products")).await.unwrap();
    assert_ne!(recreated.get_info().await.unwrap().id, id);
}
