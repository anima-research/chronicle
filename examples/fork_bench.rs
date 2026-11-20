//! Fork-cost benchmark: what does it cost to give a child a private writable
//! continuation of a parent's states, with the parent still running?
//!
//!     cargo run --release --example fork_bench               # 20,000 and 200,000 items
//!     cargo run --release --example fork_bench -- 50000      # custom sizes
//!     cargo run --release --example fork_bench -- --keep /tmp/dir 20000
//!
//! For each size N it builds a store holding one message-like AppendLog state
//! of N JSON items (~500 bytes each) under a full snapshot, plus 1,000 small
//! unrelated states, reopens it cold, and reports:
//!
//!   - parent's cold first read (the cost of ONE materialization, for scale);
//!   - head fork: all states vs a selective fork of 12 states;
//!   - a child's first read of the inherited state with the parent warm;
//!   - a child's first append and the read after it;
//!   - historical fork (`create_branch_at`): all states / selective, for a
//!     branch point after which the big state did not change, one after
//!     which it did, and one the parent has since taken a full snapshot
//!     past; plus the first read on such a branch;
//!   - resident memory growth per additional child, after the child has read
//!     the state and again after it has appended to it (10 children);
//!   - registering a field index on the big state: on the parent, on a child
//!     that has not written (copies the parent's), on one that has;
//!   - how many times anything was rebuilt from the log along the way.
//!
//! Two layouts of the big state are measured, because the first write after
//! a cold read differs between them:
//!   `snapshot+tail` — a full snapshot followed by 40 appends (the usual
//!                     shape of a live chain);
//!   `snapshot-head` — the chain head IS the full snapshot (a store that was
//!                     just compacted).
//!
//! Everything here goes through the explicit-branch (`*_on`) API, i.e. what a
//! `JsStore.view()` handle calls; the root cursor never leaves `main`.

use chronicle::{
    BranchId, Sequence, StateOperation, StateRegistration, StateSelection, StateStrategy, Store,
    StoreConfig,
};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const OTHER_STATES: usize = 1000;
const SELECTED_STATES: usize = 12;
const CHILDREN: usize = 10;
const TAIL: usize = 40;

fn rss_kb() -> u64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .expect("ps");
    String::from_utf8_lossy(&out.stdout).trim().parse().unwrap_or(0)
}

fn message(i: usize) -> serde_json::Value {
    // ~500 bytes serialized.
    json!({
        "id": format!("msg-{i:08}"),
        "role": if i % 2 == 0 { "user" } else { "assistant" },
        "timestamp": 1_750_000_000_000u64 + i as u64 * 1000,
        "channel": format!("chan-{}", i % 17),
        "content": format!("{i:08} ").repeat(44),
    })
}

fn config(path: &Path) -> StoreConfig {
    StoreConfig {
        path: path.to_path_buf(),
        blob_cache_size: 16,
        create_if_missing: true,
    }
}

fn build(path: &Path, n: usize, tail: usize) {
    let store = Store::create(config(path)).unwrap();
    store
        .register_state(StateRegistration {
            id: "messages".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 100,
                full_snapshot_every: 10,
            },
            initial_value: None,
        })
        .unwrap();
    for k in 0..OTHER_STATES {
        // The first SELECTED_STATES-1 live under "agent/a/": together with
        // "messages" they are what the selective fork inherits.
        let id = if k < SELECTED_STATES - 1 {
            format!("agent/a/state-{k:04}")
        } else {
            format!("other/state-{k:04}")
        };
        store
            .register_state(StateRegistration {
                id: id.clone(),
                strategy: StateStrategy::Snapshot,
                initial_value: None,
            })
            .unwrap();
        store
            .update_state(&id, StateOperation::Set(format!("{{\"k\":{k}}}").into_bytes()))
            .unwrap();
    }

    // Bulk load, then one full snapshot (the importer pattern).
    store.set_auto_snapshot(false);
    for i in 0..(n - tail) {
        store
            .update_state(
                "messages",
                StateOperation::Append(serde_json::to_vec(&message(i)).unwrap()),
            )
            .unwrap();
    }
    store.compact_state("messages").unwrap();
    store.set_auto_snapshot(true);
    for i in (n - tail)..n {
        store
            .update_state(
                "messages",
                StateOperation::Append(serde_json::to_vec(&message(i)).unwrap()),
            )
            .unwrap();
    }
    store.sync().unwrap();
}

fn time<T>(f: impl FnOnce() -> T) -> (T, Duration) {
    let t = Instant::now();
    let out = f();
    (out, t.elapsed())
}

fn us(d: Duration) -> String {
    let micros = d.as_secs_f64() * 1e6;
    if micros >= 10_000.0 {
        format!("{:.1} ms", micros / 1000.0)
    } else {
        format!("{:.1} us", micros)
    }
}

fn selection() -> StateSelection {
    StateSelection {
        state_ids: vec!["messages".to_string()],
        state_prefixes: vec!["agent/a/".to_string()],
    }
}

fn read_tail(store: &Store, branch: BranchId) -> usize {
    let len = store.get_state_len_on(branch, "messages").unwrap().unwrap();
    let slice = store
        .get_state_slice_on(branch, "messages", len - 100, 100)
        .unwrap()
        .unwrap();
    let first = store.get_state_item_on(branch, "messages", 0).unwrap().unwrap();
    slice.len() + first.len()
}

fn append_one(store: &Store, branch: BranchId, i: usize) {
    store
        .update_state_on(
            branch,
            "messages",
            StateOperation::Append(serde_json::to_vec(&message(i)).unwrap()),
        )
        .unwrap();
}

fn measure(path: &Path, n: usize, layout: &str) {
    let log_mb = std::fs::metadata(path.join("records.log")).unwrap().len() as f64 / 1e6;

    println!();
    println!("=== N = {n} items, layout = {layout} (records.log {log_mb:.1} MB) ===");

    let (store, opened) = time(|| Store::open(config(path)).unwrap());
    let main = store.current_branch().id;
    println!("open (rebuilds the record index from the log) ........ {}", us(opened));
    let rss_open = rss_kb();

    // --- one materialization, for scale ---------------------------------
    let (_, cold) = time(|| read_tail(&store, main));
    let rss_warm = rss_kb();
    println!("parent first read, cold (one full materialization) ... {}", us(cold));
    println!("  resident memory: {:.1} MB -> {:.1} MB (+{:.1} MB for the one materialization)",
        rss_open as f64 / 1024.0, rss_warm as f64 / 1024.0, (rss_warm - rss_open) as f64 / 1024.0);
    let recon_warm = store.state_reconstruction_count();

    // --- head forks -------------------------------------------------------
    const FORKS: usize = 200;
    let (_, all) = time(|| {
        for i in 0..FORKS {
            store.create_branch(&format!("all-{i}"), Some("main")).unwrap();
        }
    });
    let sel = selection();
    let (_, selective) = time(|| {
        for i in 0..FORKS {
            store.create_branch_with_states(&format!("sel-{i}"), Some("main"), &sel).unwrap();
        }
    });
    println!("head fork, all {} states ........................... {} each", OTHER_STATES + 1, us(all / FORKS as u32));
    println!("head fork, selective ({SELECTED_STATES} states) ...................... {} each", us(selective / FORKS as u32));
    for i in 0..FORKS {
        store.delete_branch(&format!("all-{i}")).unwrap();
        store.delete_branch(&format!("sel-{i}")).unwrap();
    }

    // --- children: read, then append --------------------------------------
    let rss_before_children = rss_kb();
    let mut children = Vec::new();
    let mut first_reads = Vec::new();
    for i in 0..CHILDREN {
        let child = store.create_branch_with_states(&format!("child-{i}"), Some("main"), &sel).unwrap().id;
        let (_, d) = time(|| read_tail(&store, child));
        first_reads.push(d);
        children.push(child);
    }
    let rss_after_reads = rss_kb();
    println!("child first read of inherited state, parent warm ..... {} (first child), {} (mean of {CHILDREN})",
        us(first_reads[0]), us(first_reads.iter().sum::<Duration>() / CHILDREN as u32));
    println!("  resident memory per child that has read: {:+.1} KB (total {:+.1} KB over {CHILDREN} children)",
        (rss_after_reads as f64 - rss_before_children as f64) / CHILDREN as f64,
        rss_after_reads as f64 - rss_before_children as f64);
    println!("  rebuilt from the log during those reads: {}", store.state_reconstruction_count() - recon_warm);

    let mut first_appends = Vec::new();
    let mut reads_after = Vec::new();
    for (i, child) in children.iter().enumerate() {
        let (_, d) = time(|| append_one(&store, *child, n + i));
        first_appends.push(d);
        let (_, d) = time(|| read_tail(&store, *child));
        reads_after.push(d);
    }
    let rss_after_appends = rss_kb();
    println!("child first append ................................... {} (first child), {} (mean of the other {})",
        us(first_appends[0]), us(first_appends[1..].iter().sum::<Duration>() / (CHILDREN - 1) as u32), CHILDREN - 1);
    println!("child read right after its first append .............. {} (mean of {CHILDREN})",
        us(reads_after.iter().sum::<Duration>() / CHILDREN as u32));
    println!("  resident memory per child after append + read: {:+.1} KB (total {:+.1} KB over {CHILDREN} children)",
        (rss_after_appends as f64 - rss_after_reads as f64) / CHILDREN as f64,
        rss_after_appends as f64 - rss_after_reads as f64);
    println!("  rebuilt from the log since the parent warmed up: {}", store.state_reconstruction_count() - recon_warm);

    // A child keeps appending: steady-state write + read.
    let child = children[0];
    let (_, steady) = time(|| {
        for i in 0..1000 {
            append_one(&store, child, n + 100 + i);
            if i % 10 == 0 {
                read_tail(&store, child);
            }
        }
    });
    println!("child steady state: 1000 appends + 100 reads ......... {} per append (crosses delta snapshots)", us(steady / 1000));
    println!("  rebuilt from the log since the parent warmed up: {}", store.state_reconstruction_count() - recon_warm);

    // The parent was not disturbed.
    let (_, parent_read) = time(|| read_tail(&store, main));
    println!("parent read after all of the above ................... {}", us(parent_read));

    // --- historical forks -------------------------------------------------
    // `unchanged`: a branch point after which "messages" did not change.
    let unchanged_at: Sequence = store.current_branch().head;
    store.update_state("agent/a/state-0000", StateOperation::Set(b"{\"k\":-1}".to_vec())).unwrap();
    let (_, hist_unchanged_all) = time(|| store.create_branch_at("hist-u-all", "main", unchanged_at).unwrap());
    let (_, hist_unchanged_sel) = time(|| store.create_branch_at_with_states("hist-u-sel", "main", unchanged_at, &sel).unwrap());
    println!("historical fork, big state UNCHANGED since the point . {} (all states), {} (selective)",
        us(hist_unchanged_all), us(hist_unchanged_sel));
    store.delete_branch("hist-u-all").unwrap();
    store.delete_branch("hist-u-sel").unwrap();

    // `changed`: the parent appended 10 messages after the branch point; its
    // last full snapshot is older than the point.
    for i in 0..5 {
        append_one(&store, main, n + 5000 + i);
    }
    let changed_at: Sequence = store.current_branch().head;
    for i in 5..15 {
        append_one(&store, main, n + 5000 + i);
    }
    let (_, hist_changed_all) = time(|| store.create_branch_at("hist-c-all", "main", changed_at).unwrap());
    let (_, hist_changed_sel) = time(|| store.create_branch_at_with_states("hist-c-sel", "main", changed_at, &sel).unwrap());
    println!("historical fork, big state CHANGED since the point ... {} (all states), {} (selective)",
        us(hist_changed_all), us(hist_changed_sel));
    // Nothing holds the value at that old chain position any more (the
    // parent moved on), so the branch's first read has to materialize it.
    let hist = store.get_branch("hist-c-sel").unwrap().id;
    let recon = store.state_reconstruction_count();
    let rss = rss_kb();
    let (_, hist_read) = time(|| read_tail(&store, hist));
    println!("first read on that historical branch ................. {} (rebuilt from the log: {}, resident {:+.1} MB)",
        us(hist_read), store.state_reconstruction_count() - recon, (rss_kb() as f64 - rss as f64) / 1024.0);
    let other = store.get_branch("hist-c-all").unwrap().id;
    let (_, other_read) = time(|| read_tail(&store, other));
    println!("first read on a second branch at the same point ...... {} (rebuilt from the log: {})",
        us(other_read), store.state_reconstruction_count() - recon - 1);

    // `snapshot since`: the parent took a full snapshot after the branch
    // point, so the walk back to the point has to pass (read) it.
    store.compact_state("messages").unwrap();
    let (_, hist_past_snapshot) = time(|| store.create_branch_at_with_states("hist-s-sel", "main", changed_at, &sel).unwrap());
    println!("historical fork, a full snapshot SINCE the point ..... {} (selective)", us(hist_past_snapshot));

    // --- field indexes are per branch --------------------------------------
    use chronicle::FieldIndexKind;
    let rss = rss_kb();
    let (_, parent_index) = time(|| store.register_state_field_index("messages", "/timestamp", FieldIndexKind::Number).unwrap());
    let rss_parent_index = rss_kb();
    let fresh = store.create_branch_with_states("idx-fresh", Some("main"), &sel).unwrap().id;
    let (_, sibling_index) = time(|| store.register_state_field_index_on(fresh, "messages", "/timestamp", FieldIndexKind::Number).unwrap());
    let rss_sibling_index = rss_kb();
    let written = store.create_branch_with_states("idx-written", Some("main"), &sel).unwrap().id;
    append_one(&store, written, n + 9000);
    let (_, own_index) = time(|| store.register_state_field_index_on(written, "messages", "/timestamp", FieldIndexKind::Number).unwrap());
    let rss_own_index = rss_kb();
    println!("field index on the big state: parent builds .......... {} (resident {:+.1} MB)",
        us(parent_index), (rss_parent_index as f64 - rss as f64) / 1024.0);
    println!("  child registers before writing (copies parent's) ... {} (resident {:+.1} MB)",
        us(sibling_index), (rss_sibling_index as f64 - rss_parent_index as f64) / 1024.0);
    println!("  child registers after writing (builds its own) ..... {} (resident {:+.1} MB)",
        us(own_index), (rss_own_index as f64 - rss_sibling_index as f64) / 1024.0);

    let (_, synced) = time(|| store.sync().unwrap());
    println!("sync ................................................. {}", us(synced));
}

/// Re-run this executable with `args`, so that each store is built and each
/// measurement taken in a process of its own: resident-memory deltas are
/// then not blurred by pages an earlier phase freed and the allocator kept.
fn in_child_process(args: &[String]) {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(args)
        .status()
        .expect("spawn self");
    assert!(status.success(), "child process failed: {args:?}");
}

fn main() {
    let mut args: Vec<String> = std::env::args().skip(1).collect();

    // Internal modes (one process each).
    match args.first().map(String::as_str) {
        Some("--build") => {
            build(Path::new(&args[1]), args[2].parse().unwrap(), args[3].parse().unwrap());
            return;
        }
        Some("--measure") => {
            measure(Path::new(&args[1]), args[2].parse().unwrap(), &args[3]);
            return;
        }
        _ => {}
    }

    let mut keep: Option<PathBuf> = None;
    if args.first().map(String::as_str) == Some("--keep") {
        keep = Some(PathBuf::from(args.remove(1)));
        args.remove(0);
    }
    let sizes: Vec<usize> = if args.is_empty() {
        vec![20_000, 200_000]
    } else {
        args.iter().map(|a| a.parse().expect("size")).collect()
    };

    let base = match &keep {
        Some(p) => p.clone(),
        None => std::env::temp_dir().join(format!("chronicle-fork-bench-{}", std::process::id())),
    };
    std::fs::create_dir_all(&base).unwrap();
    println!("stores under {}", base.display());

    for &n in &sizes {
        for (layout, tail) in [("snapshot+tail", TAIL), ("snapshot-head", 0)] {
            let path = base.join(format!("store-{n}-{layout}"));
            let _ = std::fs::remove_dir_all(&path);
            let path = path.to_string_lossy().to_string();
            in_child_process(&["--build".into(), path.clone(), n.to_string(), tail.to_string()]);
            in_child_process(&["--measure".into(), path, n.to_string(), layout.into()]);
        }
    }

    if keep.is_none() {
        let _ = std::fs::remove_dir_all(&base);
    }
}
