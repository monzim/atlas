//! Joining without the Base, and what a share uploads (ATL-402), through the
//! crate's public API: real temporary git repositories — a Base Joy never
//! pushed, in a repository with no remote — and the fake thread server, whose
//! object doors keep the real ones' limit.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use atlas_thread_sync::{
    git, run, share, Bootstrapped, FakeStore, FakeThreadServer, FakeTransport, LocalChange,
    Replica, SecretReason, ShareKind, SyncStatus, ThreadRepo, ThreadSession,
};
use tokio::sync::{mpsc, watch};

mod common;
use common::*;

/// Joy committed `older` and Alice cloned it; then Joy committed the Base on
/// top, never pushed it (her repository has no remote at all), and left work
/// uncommitted.
struct Unpushed {
    _tmp: tempfile::TempDir,
    root: PathBuf,
    joy: PathBuf,
    alice: PathBuf,
    older: String,
    base: String,
}

fn unpushed() -> Unpushed {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_path_buf();
    let joy = root.join("joy");
    init_repo(&joy);
    write(&joy, "src/banner.css", ".banner {\n  color: blue;\n}\n");
    write(&joy, "README.md", "# Site\n");
    write(&joy, ".gitignore", "dist/\n");
    git(&joy, &["add", "-A"]);
    git(&joy, &["commit", "--quiet", "-m", "older"]);
    let older = git(&joy, &["rev-parse", "HEAD"]);

    let alice = root.join("alice");
    git(
        &root,
        &["clone", "--quiet", joy.to_str().unwrap(), alice.to_str().unwrap()],
    );

    write(&joy, "src/feature.ts", "export const feature = 1;\n");
    write(&joy, "README.md", "# Site\n\nNow with a feature.\n");
    git(&joy, &["add", "-A"]);
    git(&joy, &["commit", "--quiet", "-m", "the Base, never pushed"]);
    let base = git(&joy, &["rev-parse", "HEAD"]);

    write(&joy, "src/banner.css", ".banner {\n  color: green;\n}\n");
    Unpushed {
        _tmp: tmp,
        root,
        joy,
        alice,
        older,
        base,
    }
}

/// Joy shares and keeps her replica in the app's loop, which answers bundle
/// requests.
async fn joy_shares(w: &Unpushed, server: &FakeThreadServer) -> watch::Receiver<SyncStatus> {
    let replica = Replica::new(&w.joy, &w.base, &w.root.join("replicas/joy")).unwrap();
    let mut joy = ThreadSession::open(server.connect("joy"), replica, "joy-replica-1")
        .await
        .unwrap();
    joy.set_store(Arc::new(server.store()));
    joy.set_thread_repo(ThreadRepo::at(&w.root.join("threads/joy")));
    joy.share_working_changes(&w.joy, &[]).await.unwrap();
    let (_commands, rx) = mpsc::unbounded_channel();
    let (status_tx, status) = watch::channel(SyncStatus::default());
    // The sender is leaked into the task's lifetime by moving it along.
    tokio::spawn(async move {
        let _keep = _commands;
        run(joy, rx, status_tx).await;
    });
    status
}

async fn alice_joins(
    w: &Unpushed,
    server: &FakeThreadServer,
    own: Option<&Path>,
) -> (ThreadSession<FakeTransport>, Bootstrapped) {
    let replica = Replica::without_base(&w.base, &w.root.join("replicas/alice")).unwrap();
    let mut alice = ThreadSession::open(server.connect("alice"), replica, "alice-replica-1")
        .await
        .unwrap();
    alice.set_store(Arc::new(server.store()));
    alice.set_thread_repo(ThreadRepo::at(&w.root.join("threads/alice")));
    let outcome = alice.bootstrap(own).await.unwrap();
    (alice, outcome)
}

#[tokio::test]
async fn a_joiner_with_an_older_clone_receives_only_the_missing_history_and_converges() {
    let w = unpushed();
    let server = FakeThreadServer::new();
    let _joy = joy_shares(&w, &server).await;
    assert!(!git::has_commit(&w.alice, &w.base));

    let (mut alice, outcome) = alice_joins(&w, &server, Some(&w.alice)).await;
    assert_eq!(outcome, Bootstrapped::Ready);
    assert_eq!(alice.read_only(), None);

    // A thin bundle: it needs the commit Alice already had, and nothing older
    // travelled.
    let bundles = server.store().bundles();
    assert_eq!(bundles.len(), 1);
    assert_eq!(bundles[0].1.prerequisites, vec![w.older.clone()]);

    let root = alice.materialize().unwrap();
    assert_eq!(read(&root, "src/feature.ts"), "export const feature = 1;\n");
    assert_eq!(read(&root, "README.md"), "# Site\n\nNow with a feature.\n");
    assert_eq!(
        read(&root, "src/banner.css"),
        ".banner {\n  color: green;\n}\n"
    );

    // Alice's own repository was only borrowed from, never written to.
    assert!(!git::has_commit(&w.alice, &w.base));
    assert_eq!(git(&w.alice, &["rev-parse", "HEAD"]), w.older);

    // And she can edit now: her save reaches the thread.
    write(&root, "src/banner.css", ".banner {\n  color: purple;\n}\n");
    assert!(matches!(
        alice.file_saved("src/banner.css").await.unwrap(),
        LocalChange::Update { .. }
    ));
}

#[tokio::test]
async fn a_joiner_with_no_repository_receives_a_full_bundle_and_converges() {
    let w = unpushed();
    let server = FakeThreadServer::new();
    let _joy = joy_shares(&w, &server).await;

    let (mut alice, outcome) = alice_joins(&w, &server, None).await;
    assert_eq!(outcome, Bootstrapped::Ready);
    let bundles = server.store().bundles();
    assert_eq!(bundles.len(), 1);
    assert!(bundles[0].1.prerequisites.is_empty(), "a full bundle");

    let root = alice.materialize().unwrap();
    assert_eq!(read(&root, "src/feature.ts"), "export const feature = 1;\n");
    assert_eq!(
        read(&root, "src/banner.css"),
        ".banner {\n  color: green;\n}\n"
    );

    // A later joiner with nothing is answered from the cached bundle.
    let replica = Replica::without_base(&w.base, &w.root.join("replicas/bob")).unwrap();
    let mut bob = ThreadSession::open(server.connect("bob"), replica, "bob-replica-1")
        .await
        .unwrap();
    bob.set_store(Arc::new(server.store()));
    bob.set_thread_repo(ThreadRepo::at(&w.root.join("threads/bob")));
    assert_eq!(bob.bootstrap(None).await.unwrap(), Bootstrapped::Ready);
    assert_eq!(server.store().bundles().len(), 1);
}

#[tokio::test]
async fn over_the_bundle_limit_the_joiner_watches_and_is_told_why() {
    let w = unpushed();
    let server = FakeThreadServer::new();
    server.store().set_bundle_limit(Some(64));
    let _joy = joy_shares(&w, &server).await;

    let (mut alice, outcome) = alice_joins(&w, &server, None).await;
    let Bootstrapped::WatchOnly(why) = outcome else {
        panic!("expected watch-only, got {outcome:?}");
    };
    assert!(why.contains("Base bundle limit"), "{why}");
    assert_eq!(alice.read_only().as_deref(), Some(why.as_str()));
    assert!(server.store().bundles().is_empty());

    // She still follows the thread — rebuilt from the journal alone.
    assert_eq!(
        alice.replica().text("src/banner.css").unwrap(),
        ".banner {\n  color: green;\n}\n"
    );
    // But nothing can be checked out, and nothing is sent.
    assert!(alice.materialize().is_err());
    assert_eq!(
        alice.file_saved("src/banner.css").await.unwrap(),
        LocalChange::Ignored
    );
    assert_eq!(alice.updates_sent(), 0);
}

#[tokio::test]
async fn with_nobody_online_who_has_the_base_the_joiner_watches() {
    let w = unpushed();
    let server = FakeThreadServer::new();
    let (_alice, outcome) = alice_joins(&w, &server, Some(&w.alice)).await;
    assert!(matches!(outcome, Bootstrapped::WatchOnly(why) if why.contains("online")));
}

#[tokio::test]
async fn a_joiner_who_already_has_the_base_asks_for_nothing() {
    let w = world();
    let server = FakeThreadServer::new();
    let mut monzim = open(
        &server,
        &w.monzim,
        &w.base,
        &w.replicas.join("monzim"),
        "monzim",
    )
    .await;
    monzim.set_thread_repo(ThreadRepo::at(&w.replicas.join("monzim-thread")));
    assert_eq!(
        monzim.bootstrap(Some(&w.monzim)).await.unwrap(),
        Bootstrapped::Ready
    );
    assert!(server.store().bundles().is_empty());
}

#[tokio::test]
async fn the_share_preview_lists_exactly_what_uploads_and_blocks_secrets() {
    let w = world();
    // `.atlas/shareignore` keeps a tracked file and a directory out of shares.
    write(&w.joy, ".atlas/shareignore", "README.md\nscratch/\n");
    write(&w.joy, "README.md", "# Site\n\nlocal notes\n");
    write(&w.joy, "scratch/try.txt", "experiments\n");
    write(&w.joy, "logo.png", "\u{0}PNG fake image bytes");

    let preview = share::preview(&w.joy).unwrap();
    let paths: Vec<&str> = preview.files.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(
        paths,
        vec![
            ".atlas/shareignore",
            ".env.local",
            "logo.png",
            "notes.md",
            "src/banner.css"
        ]
    );
    let env = &preview.files[1];
    assert_eq!(env.blocked, Some(SecretReason::Name));
    assert_eq!(preview.files[2].kind, ShareKind::Binary);
    assert!(preview.files.iter().all(|f| f.path == ".env.local" || f.blocked.is_none()));

    // `.env.local` stays home by default …
    let held: Vec<&str> = preview.held(&[]).map(|f| f.path.as_str()).collect();
    assert_eq!(held, vec![".env.local"]);
    // … and goes only when the person includes that file.
    let include = vec![".env.local".to_string()];
    assert!(preview.uploads(&include).any(|f| f.path == ".env.local"));
}

#[tokio::test]
async fn a_blocked_file_is_shared_only_when_included_anyway() {
    let w = world();
    let server = FakeThreadServer::new();
    let mut joy = open(&server, &w.joy, &w.base, &w.replicas.join("joy"), "joy").await;
    let report = joy
        .share_working_changes(&w.joy, &[".env.local".to_string()])
        .await
        .unwrap();
    assert!(report.blocked.is_empty());
    assert!(report.shared.contains(&".env.local".to_string()));

    let monzim = open(
        &server,
        &w.monzim,
        &w.base,
        &w.replicas.join("monzim"),
        "monzim",
    )
    .await;
    assert_eq!(
        monzim.replica().text(".env.local").unwrap(),
        "STRIPE_KEY=sk_live_not_really\n"
    );
}

#[tokio::test]
async fn ignored_files_never_sync_from_a_share_or_a_replica() {
    let w = world();
    write(&w.joy, ".atlas/shareignore", "*.log\n");
    write(&w.joy, "debug.log", "noise\n");
    git(&w.joy, &["add", ".atlas/shareignore"]);
    git(&w.joy, &["commit", "--quiet", "-m", "shareignore"]);
    let base = git(&w.joy, &["rev-parse", "HEAD"]);
    let server = FakeThreadServer::new();

    let mut joy = open(&server, &w.joy, &base, &w.replicas.join("joy"), "joy").await;
    let report = joy.share_working_changes(&w.joy, &[]).await.unwrap();
    assert!(!report.shared.iter().any(|p| p == "debug.log" || p.starts_with("dist/")));

    let root = joy.materialize().unwrap();
    write(&root, "dist/out.js", "built();\n");
    write(&root, "trace.log", "more noise\n");
    write(&root, "src/new.ts", "export {};\n");
    assert_eq!(
        joy.file_saved("dist/out.js").await.unwrap(),
        LocalChange::Ignored
    );
    assert_eq!(
        joy.file_saved("trace.log").await.unwrap(),
        LocalChange::Ignored
    );
    assert!(matches!(
        joy.file_saved("src/new.ts").await.unwrap(),
        LocalChange::NewFile { .. }
    ));
    let paths: Vec<String> = server.tree().into_iter().map(|e| e.path).collect();
    assert!(paths.contains(&"src/new.ts".to_string()));
    assert!(!paths.iter().any(|p| p.ends_with(".log") || p.starts_with("dist/")));
}

#[tokio::test]
async fn a_files_base_content_is_uploaded_when_it_enters_the_thread() {
    let w = world();
    let server = FakeThreadServer::new();
    let replica = Replica::new(&w.joy, &w.base, &w.replicas.join("joy")).unwrap();
    let mut joy = ThreadSession::open(server.connect("joy"), replica, "joy-replica-1")
        .await
        .unwrap();
    let store: FakeStore = server.store();
    joy.set_store(Arc::new(store.clone()));
    joy.share_working_changes(&w.joy, &[]).await.unwrap();
    let base = b".banner {\n  color: blue;\n}\n";
    assert_eq!(
        store.blob(&atlas_thread_sync::bootstrap::sha256_hex(base)).as_deref(),
        Some(&base[..])
    );
    // `notes.md` is new since the Base: nothing to upload for it.
    assert_eq!(store.blobs(), 1);
}
