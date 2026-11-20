// This repository's wiring of scripts/release-holds.mjs, which is a copy of
// agent-framework's: the check's own behaviour is tested with the original
// there (test/release-holds.test.ts). These cases run npm itself with this
// repository's preversion and prepublishOnly entries against a local stand-in
// for GitHub's issues endpoint, so `npm version` and `npm publish` are seen
// to stop while a hold is open and go ahead while none is, and they run the
// check with this repository's own package.json to see which repository it
// asks.
import { after, test } from "node:test";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const SCRIPT = fileURLToPath(new URL("./release-holds.mjs", import.meta.url));
const PACKAGE_JSON = fileURLToPath(new URL("../package.json", import.meta.url));

/** Answers every request with `status` and `body`, recording each request. */
async function github(status, body) {
  const seen = [];
  const server = createServer((req, res) => {
    const url = req.url ?? "";
    seen.push({ url, headers: req.headers });
    res.writeHead(status, { "content-type": "application/json" });
    res.end(JSON.stringify(body));
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address();
  return {
    url: `http://127.0.0.1:${port}`,
    seen,
    close: () => new Promise((resolve) => server.close(() => resolve())),
  };
}

const made = [];
after(() => {
  for (const dir of made) rmSync(dir, { recursive: true, force: true });
});

/** A throwaway directory with this package.json. */
function project(pkg) {
  const dir = mkdtempSync(join(tmpdir(), "release-holds-"));
  made.push(dir);
  writeFileSync(join(dir, "package.json"), JSON.stringify(pkg));
  return dir;
}

const PKG = {
  name: "fixture",
  version: "1.0.0",
  repository: { type: "git", url: "git+https://github.com/owner/repo.git" },
};

const HOLD = { number: 300, title: "Recovery drops a valid record after a torn tail" };

/** Runs `file` with `args` and only the environment given, so a CI
 *  runner's own GITHUB_REPOSITORY or token can't reach it. */
function exec(file, args, cwd, env) {
  return new Promise((resolve) => {
    execFile(file, args, { cwd, env: { PATH: process.env.PATH ?? "", ...env } }, (error, _stdout, stderr) => {
      resolve({ code: error ? Number(error.code ?? 1) : 0, stderr });
    });
  });
}

const run = (cwd, env) => exec(process.execPath, [SCRIPT], cwd, env);

/** A throwaway package whose preversion and prepublishOnly are this
 *  repository's own, with the script beside it. */

function wiredProject() {
  const { scripts } = JSON.parse(readFileSync(PACKAGE_JSON, "utf8"));
  const dir = project({
    ...PKG,
    scripts: { preversion: scripts.preversion, prepublishOnly: scripts.prepublishOnly },
  });
  mkdirSync(join(dir, "scripts"));
  copyFileSync(SCRIPT, join(dir, "scripts", "release-holds.mjs"));
  writeFileSync(join(dir, ".npmrc"), "");
  return dir;
}

/** npm, kept to the throwaway directory: its own cache and user config,
 *  and the stand-in for its registry and GitHub's API. Under `npm test`,
 *  npm names its own CLI script in npm_execpath, and this Node runs it: a
 *  bare `npm` is npm.cmd on Windows, which execFile can't start without a
 *  shell. Another package manager names its own script or binary there, so
 *  only npm's is run this way; otherwise the `npm` on PATH is used. */

function npm(dir, args, api) {
  const cli = process.env.npm_execpath;
  const [file, argv] = cli && /npm-cli\.c?js$/.test(cli) ? [process.execPath, [cli, ...args]] : ["npm", args];
  return exec(file, argv, dir, {
    HOME: dir,
    npm_config_cache: join(dir, ".npm-cache"),
    npm_config_userconfig: join(dir, ".npmrc"),
    npm_config_registry: api,
    npm_config_update_notifier: "false",
    npm_config_fund: "false",
    npm_config_audit: "false",
    GITHUB_API_URL: api,
  });
}

test("this repository's preversion stops npm version before the bump while a hold is open", async () => {
  const gh = await github(200, [HOLD]);
  try {
    const dir = wiredProject();
    const result = await npm(dir, ["version", "patch", "--no-git-tag-version"], gh.url);
    assert.notEqual(result.code, 0);
    assert.match(result.stderr, /#300 Recovery drops/);
    assert.match(result.stderr, /then run npm version again\./);
    assert.equal(JSON.parse(readFileSync(join(dir, "package.json"), "utf8")).version, "1.0.0");
  } finally {
    await gh.close();
  }
});

test("with no hold open, this repository's preversion lets npm version bump", async () => {
  const gh = await github(200, []);
  try {
    const dir = wiredProject();
    const result = await npm(dir, ["version", "patch", "--no-git-tag-version"], gh.url);
    assert.equal(result.code, 0, result.stderr);
    assert.equal(JSON.parse(readFileSync(join(dir, "package.json"), "utf8")).version, "1.0.1");
    assert.ok(gh.seen.some((s) => s.url.startsWith("/repos/owner/repo/issues?")));
  } finally {
    await gh.close();
  }
});

test("this repository's prepublishOnly stops npm publish while a hold is open", async () => {
  const gh = await github(200, [HOLD]);
  try {
    const dir = wiredProject();
    const result = await npm(dir, ["publish", "--dry-run"], gh.url);
    assert.notEqual(result.code, 0);
    assert.match(result.stderr, /#300 Recovery drops/);
    assert.match(result.stderr, /then run npm publish again\./);
  } finally {
    await gh.close();
  }
});

test("with no hold open, this repository's prepublishOnly lets npm publish go ahead", async () => {
  const gh = await github(200, []);
  try {
    const dir = wiredProject();
    const result = await npm(dir, ["publish", "--dry-run"], gh.url);
    assert.equal(result.code, 0, result.stderr);
    assert.ok(gh.seen.some((s) => s.url.startsWith("/repos/owner/repo/issues?")));
  } finally {
    await gh.close();
  }
});

test("this repository's own package.json sends the check to anima-research/chronicle", async () => {
  const gh = await github(200, []);
  try {
    const result = await run(fileURLToPath(new URL("..", import.meta.url)), { GITHUB_API_URL: gh.url, GITHUB_REPOSITORY: "fork/repo" });
    assert.equal(result.code, 0, result.stderr);
    assert.match(gh.seen[0].url, /^\/repos\/anima-research\/chronicle\/issues\?/);
  } finally {
    await gh.close();
  }
});
