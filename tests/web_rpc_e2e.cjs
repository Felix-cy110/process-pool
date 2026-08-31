const test = require("node:test");
const assert = require("node:assert/strict");
const { spawn } = require("node:child_process");
const { once } = require("node:events");
const { existsSync } = require("node:fs");
const path = require("node:path");
const { createClient, taskTemplate } = require("../web/rpc-client.js");

test("Web RPC client operates a real isolated pool: initialize, reuse, queue, expand, reject, timeout", { timeout: 15_000 }, async (t) => {
  const root = path.resolve(__dirname, "..");
  const binary = path.join(root, "target/debug/process-pool-server");
  assert.ok(existsSync(binary), "Run cargo build --bins before the Web end-to-end test");
  const server = spawn(binary, ["--listen", "127.0.0.1:0"], {
    cwd: root, env: { ...process.env, RUST_LOG: "info", NO_COLOR: "1" }, stdio: ["ignore", "pipe", "pipe"],
  });
  const exited = once(server, "exit");
  t.after(async () => {
    if (server.exitCode === null && server.signalCode === null) server.kill("SIGTERM");
    await exited;
  });
  let output = "";
  const base = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Test server did not start: ${output}`)), 5000);
    server.on("error", (error) => { clearTimeout(timer); reject(error); });
    server.once("exit", () => { clearTimeout(timer); reject(new Error(`Test server exited: ${output}`)); });
    server.stderr.on("data", (data) => { output += data; });
    server.stdout.on("data", (data) => {
      output += data;
      const match = output.match(/listen=(127\.0\.0\.1:\d+)/);
      if (match) { clearTimeout(timer); resolve(`http://${match[1]}`); }
    });
  });
  const client = createClient({ fetchImpl: (url, options) => fetch(`${base}${url}`, options) });
  const stats = async () => {
    const record = await client.call("pool.stats", {});
    assert.equal(record.status, "success");
    return record.response.result;
  };
  const waitFor = async (predicate) => {
    const deadline = Date.now() + 2000;
    do {
      const value = await stats();
      if (predicate(value)) return value;
      await new Promise((resolve) => setTimeout(resolve, 10));
    } while (Date.now() < deadline);
    assert.fail("Pool did not reach expected state");
  };

  assert.deepEqual(await stats(), { initialized: false });
  assert.equal((await fetch(`${base}/readyz`)).status, 503);
  const early = await client.call("pool.execute", taskTemplate("sum"));
  assert.equal(early.response.error.code, -32006);
  const params = { core_pool_size: 1, maximum_pool_size: 3, keep_alive_time: 100, time_unit: "milliseconds",
    work_queue: { type: "bounded", capacity: 1 }, process_factory: "echo", rejected_execution_handler: "abort" };
  const initialized = await client.call("pool.initialize", params);
  assert.equal(initialized.status, "success");
  assert.equal(initialized.response.result.worker_count, 0);
  assert.equal((await fetch(`${base}/readyz`)).status, 200);

  const warm = await client.call("pool.prestart", {});
  assert.equal(warm.response.result.started_worker_count, 1);
  assert.equal((await client.call("pool.prestart", {})).response.result.started_worker_count, 0);
  const first = await client.call("pool.execute", taskTemplate("sum"));
  const second = await client.call("pool.execute", taskTemplate("echo"));
  assert.equal(first.response.result.sum, 42);
  assert.equal(first.response.result.pid, second.response.result.pid);

  const tasks = Promise.all(Array.from({ length: 5 }, () => client.call("pool.execute", {
    payload: { op: "sleep", millis: 500 }, timeout_ms: 2000,
  })));
  const busy = await waitFor((value) => value.busy_worker_count === 3 && value.queued_task_count === 1);
  assert.equal(busy.worker_count, 3);
  const outcomes = await tasks;
  assert.equal(outcomes.filter((record) => record.status === "success").length, 4);
  assert.equal(outcomes.filter((record) => record.response?.error?.code === -32001).length, 1);

  const failed = await client.call("pool.execute", taskTemplate("fail"));
  assert.equal(failed.status, "rpc-error");
  assert.equal(failed.response.error.code, -32004);
  const timedOut = await client.call("pool.execute", { payload: { op: "sleep", millis: 200 }, timeout_ms: 20 });
  assert.equal(timedOut.response.error.code, -32003);
  const final = await waitFor((value) => value.worker_count === 1 && value.busy_worker_count === 0);
  assert.equal(final.completed_task_count, 6);
  assert.equal(final.failed_task_count, 2);
  assert.equal(final.rejected_task_count, 1);
  assert.equal((await client.call("pool.initialize", params)).response.error.code, -32007);

  for (const asset of ["rpc-client.js", "debugger.js", "dashboard.js"]) {
    const response = await fetch(`${base}/assets/${asset}`);
    assert.equal(response.status, 200);
    assert.match(response.headers.get("content-type"), /text\/javascript/);
    assert.equal(response.headers.get("cache-control"), "no-store");
  }
  const html = await (await fetch(base)).text();
  assert.ok(html.includes('id="debug-form"'));
});
