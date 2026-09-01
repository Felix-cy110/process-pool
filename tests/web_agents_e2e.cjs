const test = require("node:test");
const assert = require("node:assert/strict");
const { spawn, execFileSync } = require("node:child_process");
const { once } = require("node:events");
const { request: httpRequest } = require("node:http");
const { mkdtempSync, mkdirSync, writeFileSync, existsSync, realpathSync, rmSync } = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { createClient } = require("../web/rpc-client.js");

test("CC RPC manages real fixture processes: worktrees, reuse, permissions, interruption, restart, limits, shutdown", { timeout: 30000 }, async (t) => {
  const root = path.resolve(__dirname, "..");
  const workspace = mkdtempSync(path.join(os.tmpdir(), "process-pool-cc-test-"));
  const baseRepo = path.join(workspace, "conduit");
  mkdirSync(baseRepo);
  const git = (...args) => execFileSync("git", ["-c", "core.hooksPath=/dev/null", "-C", baseRepo, ...args], { stdio: "pipe" });
  git("init");
  git("remote", "add", "origin", "https://github.com/cogwheel0/conduit.git");
  writeFileSync(path.join(baseRepo, "fixture.txt"), "offline fixture\n");
  git("add", "fixture.txt");
  git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-m", "fixture");
  const server = spawn(path.join(root, "target/debug/process-pool-server"), ["--listen", "127.0.0.1:0",
    "--claude-program", path.join(__dirname, "fixtures/fake-claude.cjs"), "--agent-workspace-root", workspace, "--max-cc-agents", "2"],
  { cwd: root, env: { ...process.env, RUST_LOG: "info", NO_COLOR: "1" }, stdio: ["ignore", "pipe", "pipe"] });
  const exited = once(server, "exit");
  t.after(async () => {
    if (server.exitCode === null && server.signalCode === null) server.kill("SIGTERM");
    await exited;
    // Only this test's unique mkdtemp tree is removed.
    rmSync(workspace, { recursive: true, force: true });
  });
  let output = "";
  const base = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(output)), 5000);
    server.on("error", reject);
    server.stderr.on("data", (d) => { output += d; });
    server.stdout.on("data", (d) => {
      output += d;
      const match = output.match(/listen=(127\.0\.0\.1:\d+)/);
      if (match) { clearTimeout(timer); resolve(`http://${match[1]}`); }
    });
  });
  const client = createClient({ fetchImpl: (url, options) => fetch(`${base}${url}`, options) });
  const call = async (method, params = {}) => {
    const record = await client.call(method, params);
    assert.equal(record.status, "success", JSON.stringify(record.response));
    return record.response.result;
  };
  const get = (id, cursor = 0) => call("cc.get", { agent_id: id, after_event_id: cursor });
  const target = (agent) => ({ agent_id: agent.id, generation: agent.generation });
  const wait = async (id, predicate) => {
    for (let i = 0; i < 300; i++) {
      const snapshot = await get(id);
      if (predicate(snapshot)) return snapshot;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.fail(`Agent did not reach expected state: ${JSON.stringify(await get(id))}`);
  };
  const state = (expected) => (snapshot) => snapshot.agent.state === expected;

  assert.deepEqual((await call("cc.status")).agents, []);
  assert.match((await call("cc.prepare")).claude_version, /fake Claude/);
  assert.equal((await client.call("cc.create", { cwd: "/", label: "bad" })).status, "rpc-error");
  assert.equal((await client.call("cc.send", { agent_id: "missing", generation: 1, prompt: "" })).status, "rpc-error");
  let first = await call("cc.create", { label: "会话一" });
  await wait(first.id, state("idle"));
  let second = await call("cc.create", { label: "会话二" });
  await wait(second.id, state("idle"));
  assert.equal(first.slot, 1);
  assert.equal(second.slot, 2);
  assert.notEqual(first.pid, second.pid);
  assert.notEqual(first.cwd, second.cwd);
  assert.ok(first.cwd.startsWith(workspace));
  assert.ok(existsSync(path.join(first.cwd, "fixture.txt")));
  writeFileSync(path.join(first.cwd, "user-work.txt"), "preserve me\n");
  assert.ok(!existsSync(path.join(second.cwd, "user-work.txt")));
  const capped = await client.call("cc.create", { label: "超限" });
  assert.match(capped.errorMessage, /上限/);

  for (let round = 1; round <= 2; round++) {
    await call("cc.send", { ...target(first), prompt: "hello" });
    const snapshot = await wait(first.id, (s) => s.agent.completed_turns === round);
    assert.equal(snapshot.agent.pid, first.pid);
    const last = snapshot.events.filter((e) => e.data.type === "result").at(-1);
    const result = JSON.parse(last.data.result);
    assert.equal(result.turn, round);
    assert.equal(result.cwd, realpathSync(first.cwd));
    first = snapshot.agent;
  }
  assert.deepEqual((await get(first.id, (await get(first.id)).cursor)).events, []);

  for (const allow of [false, true]) {
    const before = first.completed_turns;
    await call("cc.send", { ...target(first), prompt: "permission" });
    const pending = await wait(first.id, state("awaiting_permission"));
    assert.equal(pending.agent.pending_permissions["permission-1"].tool_name, "Bash");
    assert.equal((await client.call("cc.permission", { ...target(first), request_id: "wrong", allow })).status, "rpc-error");
    await call("cc.permission", { ...target(first), request_id: "permission-1", allow });
    const done = await wait(first.id, (s) => s.agent.completed_turns > before);
    assert.deepEqual(done.agent.pending_permissions, {});
    assert.ok(done.events.some((e) => e.data.result === `permission:${allow ? "allow" : "deny"}`));
    first = done.agent;
  }
  await call("cc.send", { ...target(first), prompt: "hang" });
  const busy = await wait(first.id, state("busy"));
  assert.equal(busy.agent.current_task, "hang");
  assert.equal((await client.call("cc.send", { ...target(first), prompt: "duplicate" })).status, "rpc-error");
  await call("cc.interrupt", target(first));
  assert.equal((await wait(first.id, state("idle"))).agent.current_task, null);
  assert.equal((await get(first.id)).agent.pid, first.pid);
  const childBefore = (await get(first.id)).agent.completed_turns;
  await call("cc.send", { ...target(first), prompt: "child" });
  const childResult = await wait(first.id, (s) => s.agent.completed_turns > childBefore);
  const childPid = JSON.parse(childResult.events.filter((e) => e.data.type === "result").at(-1).data.result).child_pid;
  assert.ok(Number.isInteger(childPid));
  process.kill(childPid, 0);
  await call("cc.stop", target(first));
  assert.ok(existsSync(path.join(first.cwd, "user-work.txt")), "stop retains user files");
  assert.throws(() => process.kill(first.pid, 0), /ESRCH/);
  // Descendants may need a short OS reaping interval after the group is killed.
  for (let i = 0; i < 100; i++) {
    try { process.kill(childPid, 0); } catch { break; }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  assert.throws(() => process.kill(childPid, 0), /ESRCH/);
  const old = first;
  first = await call("cc.restart", target(first));
  await wait(first.id, state("idle"));
  assert.equal(first.generation, old.generation + 1);
  assert.equal(first.slot, old.slot);
  assert.notEqual(first.pid, old.pid);
  assert.equal(first.cwd, old.cwd);
  assert.equal(first.session_id, old.session_id);
  assert.equal((await client.call("cc.stop", target(old))).status, "rpc-error");
  const before = first.completed_turns;
  await call("cc.send", { ...target(first), prompt: "after resume" });
  assert.equal((await wait(first.id, (s) => s.agent.completed_turns > before)).agent.session_id, old.session_id);

  await call("cc.send", { ...target(second), prompt: "flood" });
  const flood = await wait(second.id, (s) => s.agent.completed_turns === 1);
  assert.equal(flood.truncated, true);
  assert.equal(flood.events.length, 256);
  await call("cc.send", { ...target(second), prompt: "huge-event" });
  const huge = await wait(second.id, (s) => s.agent.completed_turns === 2);
  assert.ok(huge.events.some((e) => e.data.truncated && e.data.preview));
  await call("cc.send", { ...target(second), prompt: "oversized" });
  assert.match((await wait(second.id, state("failed"))).agent.last_error, /1 MiB/);
  second = await call("cc.restart", target(second));
  await wait(second.id, state("idle"));
  await call("cc.send", { ...target(second), prompt: "crash" });
  const crashed = await wait(second.id, state("failed"));
  assert.match(crashed.agent.last_error, /退出|输出已关闭/);
  assert.equal(crashed.agent.current_task, "crash", "failed slot retains the work that was running");
  const released = await call("cc.stop", target(crashed.agent));
  assert.equal(released.state, "stopped");
  assert.equal(released.current_task, null);
  const replacement = await call("cc.create", { label: "复用释放后的槽位" });
  assert.equal(replacement.slot, second.slot);
  await wait(replacement.id, state("idle"));
  await call("cc.stop", target(replacement));

  for (const headers of [{ origin: "https://attacker.invalid" }, { origin: "null" }, { "sec-fetch-site": "cross-site" }, { host: "attacker.invalid" }, { origin: "http://127.0.0.1:1" }]) {
    // Node fetch normalizes Host; raw HTTP ensures the rebinding test sends it literally.
    const response = await new Promise((resolve, reject) => {
      const request = httpRequest(`${base}/rpc`, { method: "POST", headers: { "content-type": "application/json", ...headers } }, (response) => {
        let body = ""; response.on("data", (chunk) => { body += chunk; }); response.on("end", () => resolve(JSON.parse(body)));
      });
      request.on("error", reject);
      request.end(JSON.stringify({ jsonrpc: "2.0", id: "security", method: "cc.create", params: {} }));
    });
    assert.equal(response.error?.code, -32101, JSON.stringify({ headers, response }));
  }
  const sameOrigin = await fetch(`${base}/rpc`, { method: "POST", headers: { "content-type": "application/json", origin: base, "sec-fetch-site": "same-origin" }, body: JSON.stringify({ jsonrpc: "2.0", id: "same", method: "cc.status", params: {} }) });
  assert.equal((await sameOrigin.json()).result.enabled, true);
  assert.deepEqual(await call("pool.stats"), { initialized: false }, "CC is independent of the generic pool");
  server.kill("SIGTERM");
  await exited;
  assert.throws(() => process.kill(first.pid, 0), /ESRCH/, "server shutdown reaps managed CC");
  assert.ok(existsSync(path.join(first.cwd, "user-work.txt")));
});
