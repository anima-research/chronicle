#!/usr/bin/env bash
# Cross-version store-compatibility gate (issue #15, acceptance test 1).
#
# Proves a store written by this working tree remains fully usable by an
# unmodified chronicle v0.2.6 build — open, materialize, time-travel,
# branch, keep writing — and vice versa. The same store directory passes
# through four phases, alternating binaries; see
# scripts/cross-version-compat/main.rs for the choreography.
#
# Usage: scripts/cross-version-compat.sh [old-ref]   (default v0.2.6)
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OLD_REF="${1:-v0.2.6}"
WORK="$REPO/target/cross-version-compat"
OLD_TREE="$WORK/chronicle-$OLD_REF"
STORE="$WORK/store"

cleanup() { git -C "$REPO" worktree remove --force "$OLD_TREE" 2>/dev/null || true; }
trap cleanup EXIT

mkdir -p "$WORK"
rm -rf "$STORE" "$WORK/store.checkpoint.json"
cleanup
git -C "$REPO" worktree add --detach "$OLD_TREE" "$OLD_REF" >/dev/null

harness() { # $1 = old|new, $2 = chronicle path
  local dir="$WORK/harness-$1"
  mkdir -p "$dir/src"
  cp "$REPO/scripts/cross-version-compat/main.rs" "$dir/src/main.rs"
  cat >"$dir/Cargo.toml" <<EOF
[package]
name = "compat-$1"
version = "0.0.0"
edition = "2021"

[features]
new-api = []

[dependencies]
chronicle = { path = "$2", default-features = false }
serde_json = "1.0"
EOF
}

harness old "$OLD_TREE"
harness new "$REPO"

echo "== building harnesses against $OLD_REF and the working tree =="
(cd "$WORK/harness-old" && cargo build --quiet --release)
(cd "$WORK/harness-new" && cargo build --quiet --release --features new-api)

OLD_BIN="$WORK/harness-old/target/release/compat-old"
NEW_BIN="$WORK/harness-new/target/release/compat-new"

echo "== phase A: $OLD_REF creates and populates the store =="
"$OLD_BIN" create "$STORE"

echo "== phase B: working tree opens, retunes cadence, extends =="
"$NEW_BIN" extend "$STORE"

test -f "$STORE/state.bin.baselines" ||
  { echo "FAIL: baselines sidecar missing after working-tree writes"; exit 1; }

echo "== phase C: unmodified $OLD_REF reopens the store the new build wrote =="
"$OLD_BIN" verify-old "$STORE"

echo "== phase D: working tree reopens after the $OLD_REF rollback wrote =="
"$NEW_BIN" verify-new "$STORE"

echo "PASS: cross-version compatibility ($OLD_REF <-> working tree)"
