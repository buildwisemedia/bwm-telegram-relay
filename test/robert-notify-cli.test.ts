import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const SCRIPT = fileURLToPath(new URL("../bin/bwm-robert-notify", import.meta.url));

// A fake curl that records its argv (one per line) and, when headers come in
// on stdin (-H @-), what it read there. It answers like the relay does.
const FAKE_CURL = `#!/bin/sh
printf '%s\\n' "$@" > "$FAKE_CURL_DIR/argv"
case " $* " in *" @- "*) cat > "$FAKE_CURL_DIR/stdin";; esac
printf '{"ok":true}\\n%s' "\${FAKE_CURL_CODE:-200}"
`;

type Sandbox = { root: string; home: string; vault: string; record: string; env: NodeJS.ProcessEnv };

function sandbox(): Sandbox {
  const root = mkdtempSync(join(tmpdir(), "robert-notify-"));
  const home = join(root, "home");
  const bin = join(root, "bin");
  const record = join(root, "record");
  mkdirSync(join(home, ".bwm_secrets"), { recursive: true });
  mkdirSync(bin);
  mkdirSync(record);
  writeFileSync(join(bin, "curl"), FAKE_CURL);
  chmodSync(join(bin, "curl"), 0o755);
  const env: NodeJS.ProcessEnv = {
    PATH: `${bin}:${process.env.PATH ?? "/usr/bin:/bin"}`,
    HOME: home,
    FAKE_CURL_DIR: record,
    BWM_TELEGRAM_RELAY_URL: "https://relay.invalid",
  };
  return { root, home, vault: join(home, ".bwm_secrets", "ea-substrate.env"), record, env };
}

function writeVault(path: string, body: string, mode = 0o600) {
  writeFileSync(path, body);
  chmodSync(path, mode);
}

function writeSettings(box: Sandbox, key: string) {
  mkdirSync(join(box.home, ".claude"), { recursive: true });
  writeFileSync(join(box.home, ".claude", "settings.json"), JSON.stringify({ env: { BWM_INTERNAL_KEY: key } }));
}

// The timeout turns a hang (for example a blocking vault open) into a failure.
function run(box: Sandbox, args: string[], extra: NodeJS.ProcessEnv = {}) {
  return spawnSync("bash", [SCRIPT, ...args], { env: { ...box.env, ...extra }, encoding: "utf8", timeout: 20_000 });
}

const FYI = ["--type", "fyi", "--punchline", "test line"];

test("reads the key from the vault and sends it on stdin, never on argv", () => {
  const box = sandbox();
  try {
    writeVault(box.vault, "OTHER=1\nexport BWM_INTERNAL_KEY='vault-key-123'\n");
    const r = run(box, FYI);
    assert.equal(r.status, 0, r.stderr);
    const argv = readFileSync(join(box.record, "argv"), "utf8");
    assert.ok(!argv.includes("vault-key-123"), "key leaked onto curl argv");
    assert.ok(argv.split("\n").includes("@-"), "curl did not read headers from stdin");
    assert.equal(readFileSync(join(box.record, "stdin"), "utf8").trim(), "X-BWM-Internal-Key: vault-key-123");
    assert.ok(argv.includes("https://relay.invalid/notify"));
  } finally {
    rmSync(box.root, { recursive: true, force: true });
  }
});

test("an exported key wins over the vault", () => {
  const box = sandbox();
  try {
    writeVault(box.vault, "BWM_INTERNAL_KEY=vault-key\n");
    const r = run(box, FYI, { BWM_INTERNAL_KEY: "env-key" });
    assert.equal(r.status, 0, r.stderr);
    assert.equal(readFileSync(join(box.record, "stdin"), "utf8").trim(), "X-BWM-Internal-Key: env-key");
    assert.ok(!readFileSync(join(box.record, "argv"), "utf8").includes("env-key"));
  } finally {
    rmSync(box.root, { recursive: true, force: true });
  }
});

test("BWM_INTERNAL_KEY_FILE points at another vault file", () => {
  const box = sandbox();
  try {
    const other = join(box.home, ".bwm_secrets", "other.env");
    writeVault(other, "BWM_INTERNAL_KEY=other-key\n");
    const r = run(box, FYI, { BWM_INTERNAL_KEY_FILE: other });
    assert.equal(r.status, 0, r.stderr);
    assert.equal(readFileSync(join(box.record, "stdin"), "utf8").trim(), "X-BWM-Internal-Key: other-key");
  } finally {
    rmSync(box.root, { recursive: true, force: true });
  }
});

test("ignores a vault file with group or other access", () => {
  const box = sandbox();
  try {
    writeVault(box.vault, "BWM_INTERNAL_KEY=open-key\n", 0o644);
    const r = run(box, FYI);
    assert.equal(r.status, 78);
    assert.match(r.stderr, /permissions too open/);
    assert.ok(!existsSync(join(box.record, "argv")), "curl ran without a usable key");
    assert.ok(!r.stderr.includes("open-key") && !r.stdout.includes("open-key"));
  } finally {
    rmSync(box.root, { recursive: true, force: true });
  }
});

test("ignores a symlinked vault file", () => {
  const box = sandbox();
  try {
    const target = join(box.root, "target.env");
    writeVault(target, "BWM_INTERNAL_KEY=link-key\n");
    symlinkSync(target, box.vault);
    const r = run(box, FYI);
    assert.equal(r.status, 78);
    assert.ok(!existsSync(join(box.record, "argv")));
  } finally {
    rmSync(box.root, { recursive: true, force: true });
  }
});

test("falls back to the legacy settings.json env block", () => {
  const box = sandbox();
  try {
    mkdirSync(join(box.home, ".claude"));
    writeFileSync(join(box.home, ".claude", "settings.json"), JSON.stringify({ env: { BWM_INTERNAL_KEY: "settings-key" } }));
    const r = run(box, FYI);
    assert.equal(r.status, 0, r.stderr);
    assert.equal(readFileSync(join(box.record, "stdin"), "utf8").trim(), "X-BWM-Internal-Key: settings-key");
  } finally {
    rmSync(box.root, { recursive: true, force: true });
  }
});

test("exits 78 without calling curl when no key exists anywhere", () => {
  const box = sandbox();
  try {
    const r = run(box, FYI);
    assert.equal(r.status, 78);
    assert.match(r.stderr, /BWM_INTERNAL_KEY not found/);
    assert.ok(!existsSync(join(box.record, "argv")));
  } finally {
    rmSync(box.root, { recursive: true, force: true });
  }
});

test("--dry-run reports the body and key source, never the key, and sends nothing", () => {
  const box = sandbox();
  try {
    writeVault(box.vault, "BWM_INTERNAL_KEY=dry-key\n");
    const r = run(box, [...FYI, "--dry-run"]);
    assert.equal(r.status, 0, r.stderr);
    assert.ok(!existsSync(join(box.record, "argv")), "dry run called curl");
    assert.ok(!r.stdout.includes("dry-key") && !r.stderr.includes("dry-key"));
    const out = JSON.parse(r.stdout);
    assert.equal(out.dry_run, true);
    assert.equal(out.key_source, "vault");
    assert.equal(out.relay, "https://relay.invalid/notify");
    assert.equal(out.body.type, "fyi");
    assert.equal(out.body.punchline, "test line");
  } finally {
    rmSync(box.root, { recursive: true, force: true });
  }
});

test("--dry-run still exits 78 when no key exists", () => {
  const box = sandbox();
  try {
    const r = run(box, [...FYI, "--dry-run"]);
    assert.equal(r.status, 78);
  } finally {
    rmSync(box.root, { recursive: true, force: true });
  }
});

test("--help works without a key", () => {
  const box = sandbox();
  try {
    const r = run(box, ["--help"]);
    assert.equal(r.status, 0);
    assert.match(r.stdout, /Exit: 0 sent\/queued/);
  } finally {
    rmSync(box.root, { recursive: true, force: true });
  }
});

test("a directory at the vault path falls back to settings.json", () => {
  const box = sandbox();
  try {
    mkdirSync(box.vault);
    writeSettings(box, "settings-key");
    const r = run(box, FYI);
    assert.equal(r.status, 0, r.stderr);
    assert.match(r.stderr, /not a regular file/);
    assert.equal(readFileSync(join(box.record, "stdin"), "utf8").trim(), "X-BWM-Internal-Key: settings-key");
  } finally {
    rmSync(box.root, { recursive: true, force: true });
  }
});

test("a FIFO at the vault path falls back without blocking", () => {
  const box = sandbox();
  try {
    assert.equal(spawnSync("mkfifo", [box.vault]).status, 0, "mkfifo failed");
    writeSettings(box, "settings-key");
    const r = run(box, FYI);
    assert.equal(r.status, 0, r.stderr);
    assert.match(r.stderr, /not a regular file/);
    assert.equal(readFileSync(join(box.record, "stdin"), "utf8").trim(), "X-BWM-Internal-Key: settings-key");
  } finally {
    rmSync(box.root, { recursive: true, force: true });
  }
});

test("a vault file that is not UTF-8 falls back without echoing its bytes", () => {
  const box = sandbox();
  try {
    writeFileSync(box.vault, Buffer.concat([Buffer.from("BWM_INTERNAL_KEY=zq-secret-"), Buffer.from([0xff, 0xfe]), Buffer.from("\n")]));
    chmodSync(box.vault, 0o600);
    writeSettings(box, "settings-key");
    const r = run(box, FYI);
    assert.equal(r.status, 0, r.stderr);
    assert.match(r.stderr, /unreadable \(UnicodeDecodeError\)/);
    assert.ok(!r.stderr.includes("zq-secret"), "vault bytes echoed on stderr");
    assert.equal(readFileSync(join(box.record, "stdin"), "utf8").trim(), "X-BWM-Internal-Key: settings-key");
  } finally {
    rmSync(box.root, { recursive: true, force: true });
  }
});

test("a relay error still exits 1", () => {
  const box = sandbox();
  try {
    writeVault(box.vault, "BWM_INTERNAL_KEY=k\n");
    const r = run(box, FYI, { FAKE_CURL_CODE: "401" });
    assert.equal(r.status, 1);
    assert.match(r.stderr, /relay returned HTTP 401/);
  } finally {
    rmSync(box.root, { recursive: true, force: true });
  }
});
