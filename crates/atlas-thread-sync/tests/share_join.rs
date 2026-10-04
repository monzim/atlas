//! Sharing and joining a Shared Thread when the Base is already local (ATL-395),
//! through the crate's public API: real temporary git repositories, and the
//! in-process fake thread server that keeps the real one's rules.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use atlas_thread_sync::{
    run, Command as SyncCommand, FakeThreadServer, FakeTransport, LocalChange, Replica,
    ReplicaError, SyncStatus, ThreadSession,
};
use tokio::sync::{mpsc, oneshot, watch};

const QUIET: Duration = Duration::from_millis(50);

fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .expect("run git");
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn init_repo(dir: &Path) {
    fs::create_dir_all(dir).unwrap();
    git(dir, &["init", "--quiet", "--initial-branch=main"]);
    git(dir, &["config", "user.name", "Atlas Test"]);
    git(dir, &["config", "user.email", "test@atlas.invalid"]);
}

fn write(dir: &Path, rel: &str, content: &str) {
    let target = dir.join(rel);
    fs::create_dir_all(target.parent().unwrap()).unwrap();
    fs::write(target, content).unwrap();
}

fn read(dir: &Path, rel: &str) -> String {
    fs::read_to_string(dir.join(rel)).unwrap()
}

/// Joy's repository with a committed Base and some uncommitted work on top,
/// and Monzim's clone of it — so both hold the Base.
struct World {
    _tmp: tempfile::TempDir,
    joy: PathBuf,
    monzim: PathBuf,
    base: String,
    replicas: PathBuf,
}

fn world() -> World {
    let tmp = tempfile::tempdir().unwrap();
    let joy = tmp.path().join("joy");
    init_repo(&joy);
    write(&joy, "src/banner.css", ".banner {\n  color: blue;\n}\n");
    write(&joy, "README.md", "# Site\n");
    write(&joy, ".gitignore", "dist/\n");
    git(&joy, &["add", "-A"]);
    git(&joy, &["commit", "--quiet", "-m", "base"]);
    let base = git(&joy, &["rev-parse", "HEAD"]);

    let monzim = tmp.path().join("monzim");
    git(
        tmp.path(),
        &[
            "clone",
            "--quiet",
            joy.to_str().unwrap(),
            monzim.to_str().unwrap(),
        ],
    );

    // Joy's uncommitted work: an edit, a new file, and build output that
    // must never sync.
    write(&joy, "src/banner.css", ".banner {\n  color: green;\n}\n");
    write(&joy, "notes.md", "todo: red?\n");
    write(&joy, "dist/bundle.js", "minified();\n");

    let replicas = tmp.path().join("replicas");
    World {
        _tmp: tmp,
        joy,
        monzim,
        base,
        replicas,
    }
}

async fn open(
    server: &FakeThreadServer,
    repo: &Path,
    base: &str,
    root: &Path,
    user: &str,
) -> ThreadSession<FakeTransport> {
    let replica = Replica::new(repo, base, root).unwrap();
    ThreadSession::open(server.connect(user), replica, &format!("{user}-replica-1"))
        .await
        .unwrap()
}

#[tokio::test]
async fn sharer_work_reaches_a_joiner_who_already_has_the_base() {
    let w = world();
    let server = FakeThreadServer::new();

    let mut joy = open(&server, &w.joy, &w.base, &w.replicas.join("joy"), "joy").await;
    let mut shared = joy.share_working_changes(&w.joy).await.unwrap();
    shared.sort();
    assert_eq!(
        shared,
        vec!["notes.md".to_string(), "src/banner.css".to_string()]
    );

    let monzim_root = w.replicas.join("monzim");
    let mut monzim = open(&server, &w.monzim, &w.base, &monzim_root, "monzim").await;
    assert_eq!(
        monzim.replica().text("src/banner.css").unwrap(),
        ".banner {\n  color: green;\n}\n"
    );
    assert_eq!(monzim.replica().text("notes.md").unwrap(), "todo: red?\n");

    // Nothing is checked out until the joiner opens a file or prompts.
    assert!(!monzim_root.exists());
    assert!(!monzim.replica().is_materialized());

    let root = monzim.materialize().unwrap();
    assert_eq!(
        read(&root, "src/banner.css"),
        ".banner {\n  color: green;\n}\n"
    );
    assert_eq!(read(&root, "notes.md"), "todo: red?\n");
    assert_eq!(read(&root, "README.md"), "# Site\n");
    assert!(!root.join("dist/bundle.js").exists());

    // The person's own checkouts were never touched.
    assert_eq!(
        read(&w.monzim, "src/banner.css"),
        ".banner {\n  color: blue;\n}\n"
    );
    assert_eq!(
        read(&w.joy, "src/banner.css"),
        ".banner {\n  color: green;\n}\n"
    );
}

#[tokio::test]
async fn saves_from_any_editor_converge_both_ways_without_echo() {
    let w = world();
    let server = FakeThreadServer::new();
    let mut joy = open(&server, &w.joy, &w.base, &w.replicas.join("joy"), "joy").await;
    joy.share_working_changes(&w.joy).await.unwrap();
    let mut monzim = open(
        &server,
        &w.monzim,
        &w.base,
        &w.replicas.join("monzim"),
        "monzim",
    )
    .await;
    let joy_root = joy.materialize().unwrap();
    let monzim_root = monzim.materialize().unwrap();

    // Monzim saves from an external editor.
    write(
        &monzim_root,
        "src/banner.css",
        ".banner {\n  color: red;\n}\n",
    );
    assert!(matches!(
        monzim.file_saved("src/banner.css").await.unwrap(),
        LocalChange::Update { .. }
    ));
    joy.pump(QUIET).await.unwrap();
    assert_eq!(
        read(&joy_root, "src/banner.css"),
        ".banner {\n  color: red;\n}\n"
    );

    // Joy's watcher would now report the file her replica just wrote. It is
    // her own echo: nothing is sent.
    let sent = server.updates_received();
    assert_eq!(
        joy.file_saved("src/banner.css").await.unwrap(),
        LocalChange::Echo
    );
    assert_eq!(server.updates_received(), sent);

    // Joy edits the other file and creates a new one; both reach Monzim's disk.
    write(&joy_root, "notes.md", "todo: red, done\n");
    joy.file_saved("notes.md").await.unwrap();
    write(&joy_root, "src/hero.css", ".hero { margin: 0; }\n");
    assert_eq!(
        joy.file_saved("src/hero.css").await.unwrap(),
        LocalChange::NewFile {
            path: "src/hero.css".into()
        }
    );
    monzim.pump(QUIET).await.unwrap();
    assert_eq!(read(&monzim_root, "notes.md"), "todo: red, done\n");
    assert_eq!(read(&monzim_root, "src/hero.css"), ".hero { margin: 0; }\n");
    assert_eq!(
        monzim.file_saved("notes.md").await.unwrap(),
        LocalChange::Echo
    );
    assert_eq!(monzim.gaps(), 0);
}

#[tokio::test]
async fn concurrent_saves_to_one_file_merge() {
    let w = world();
    let server = FakeThreadServer::new();
    let mut joy = open(&server, &w.joy, &w.base, &w.replicas.join("joy"), "joy").await;
    joy.share_working_changes(&w.joy).await.unwrap();
    let mut monzim = open(
        &server,
        &w.monzim,
        &w.base,
        &w.replicas.join("monzim"),
        "monzim",
    )
    .await;
    let joy_root = joy.materialize().unwrap();
    let monzim_root = monzim.materialize().unwrap();

    // Both save before either has heard from the other.
    write(
        &joy_root,
        "src/banner.css",
        "/* joy */\n.banner {\n  color: green;\n}\n",
    );
    write(
        &monzim_root,
        "src/banner.css",
        ".banner {\n  color: green;\n}\n/* monzim */\n",
    );
    joy.file_saved("src/banner.css").await.unwrap();
    monzim.file_saved("src/banner.css").await.unwrap();
    joy.pump(QUIET).await.unwrap();
    monzim.pump(QUIET).await.unwrap();

    let merged = "/* joy */\n.banner {\n  color: green;\n}\n/* monzim */\n";
    assert_eq!(read(&joy_root, "src/banner.css"), merged);
    assert_eq!(read(&monzim_root, "src/banner.css"), merged);
}

#[tokio::test]
async fn a_save_racing_a_remote_change_is_kept() {
    let w = world();
    let server = FakeThreadServer::new();
    let mut joy = open(&server, &w.joy, &w.base, &w.replicas.join("joy"), "joy").await;
    joy.share_working_changes(&w.joy).await.unwrap();
    let mut monzim = open(
        &server,
        &w.monzim,
        &w.base,
        &w.replicas.join("monzim"),
        "monzim",
    )
    .await;
    let joy_root = joy.materialize().unwrap();
    let monzim_root = monzim.materialize().unwrap();

    write(&joy_root, "notes.md", "todo: red?\njoy was here\n");
    joy.file_saved("notes.md").await.unwrap();
    // Monzim saved before his watcher reported it, and the remote change
    // arrives first: his save must survive the write-back.
    write(&monzim_root, "notes.md", "monzim was here\ntodo: red?\n");
    monzim.pump(QUIET).await.unwrap();
    let after = read(&monzim_root, "notes.md");
    assert!(
        after.contains("joy was here") && after.contains("monzim was here"),
        "{after}"
    );
    joy.pump(QUIET).await.unwrap();
    assert_eq!(read(&joy_root, "notes.md"), after);
}

#[tokio::test]
async fn a_repository_without_the_base_is_refused() {
    let w = world();
    let stranger = w.replicas.parent().unwrap().join("stranger");
    init_repo(&stranger);
    write(&stranger, "x", "x");
    git(&stranger, &["add", "-A"]);
    git(&stranger, &["commit", "--quiet", "-m", "unrelated"]);
    assert!(matches!(
        Replica::new(&stranger, &w.base, &w.replicas.join("s")),
        Err(ReplicaError::BaseMissing(_))
    ));
}

#[tokio::test]
async fn a_reconnecting_replica_does_not_reuse_client_seqs() {
    let w = world();
    let server = FakeThreadServer::new();
    let mut joy = open(&server, &w.joy, &w.base, &w.replicas.join("joy"), "joy").await;
    joy.share_working_changes(&w.joy).await.unwrap();
    let head = server.head();
    drop(joy);

    // Same replica id: the welcome says how far the server got, and new frames
    // continue above it rather than being acked as duplicates and lost.
    let mut again = open(&server, &w.joy, &w.base, &w.replicas.join("joy"), "joy").await;
    let root = again.materialize().unwrap();
    write(&root, "notes.md", "after reconnect\n");
    again.file_saved("notes.md").await.unwrap();
    again.pump(QUIET).await.unwrap();
    assert_eq!(server.head(), head + 1);
}

/// The whole loop, with real file watchers: an external save on one side
/// lands on the other's disk, and the write it causes there is not echoed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_run_loop_syncs_watched_saves_and_never_echoes() {
    let w = world();
    let server = FakeThreadServer::new();
    let mut joy = open(&server, &w.joy, &w.base, &w.replicas.join("joy"), "joy").await;
    joy.share_working_changes(&w.joy).await.unwrap();
    let monzim = open(
        &server,
        &w.monzim,
        &w.base,
        &w.replicas.join("monzim"),
        "monzim",
    )
    .await;

    let spawn = |session: ThreadSession<FakeTransport>| {
        let (commands, rx) = mpsc::channel(8);
        let (status_tx, status) = watch::channel(SyncStatus::default());
        tokio::spawn(run(session, rx, status_tx));
        (commands, status)
    };
    let materialize = |commands: mpsc::Sender<SyncCommand>| async move {
        let (reply, answer) = oneshot::channel();
        commands
            .send(SyncCommand::Materialize(reply))
            .await
            .unwrap();
        answer.await.unwrap().unwrap()
    };

    let (joy_cmd, _joy_status) = spawn(joy);
    let (monzim_cmd, _monzim_status) = spawn(monzim);
    let joy_root = materialize(joy_cmd.clone()).await;
    let monzim_root = materialize(monzim_cmd.clone()).await;

    write(
        &monzim_root,
        "src/banner.css",
        ".banner {\n  color: hotpink;\n}\n",
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while read(&joy_root, "src/banner.css") != ".banner {\n  color: hotpink;\n}\n" {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the save never reached the other replica"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    // Give both watchers time to report the writes they caused.
    let settled = server.updates_received();
    tokio::time::sleep(Duration::from_millis(750)).await;
    assert_eq!(
        server.updates_received(),
        settled,
        "a remote write was echoed back"
    );

    joy_cmd.send(SyncCommand::Stop).await.unwrap();
    monzim_cmd.send(SyncCommand::Stop).await.unwrap();
}
