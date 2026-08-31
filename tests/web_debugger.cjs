const test = require("node:test");
const assert = require("node:assert/strict");
const { readFileSync } = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const rpc = require("../web/rpc-client.js");
const { mount } = require("../web/debugger.js");

function response(request, message = { result: { pid: 42, sum: 42 } }) {
  return new Response(JSON.stringify({ jsonrpc: "2.0", id: request.id, ...message }), {
    headers: { "content-type": "application/json" },
  });
}

test("parameters and batch bounds reject invalid input without sending", () => {
  for (const text of ["{", "null", "[]", "3", '"hello"']) assert.throws(() => rpc.parseParams(text));
  assert.deepEqual(rpc.parseParams('{"payload":null}'), { payload: null });
  assert.throws(() => rpc.parseParams(JSON.stringify({ value: "中".repeat(30_000) })), /64 KiB/);
  for (const count of [0, 17, 1.5, NaN]) assert.throws(() => rpc.validateCount("pool.execute", count));
  assert.throws(() => rpc.validateCount("pool.initialize", 2));
  assert.throws(() => rpc.validateCount("unknown", 1));
  rpc.validateCount("pool.execute", 16);
});

test("same-origin RPC envelopes have unique IDs, immutable params, and full responses", async () => {
  const requests = [];
  const states = [];
  const client = rpc.createClient({ fetchImpl: async (url, options) => {
    assert.equal(url, "/rpc");
    assert.equal(options.method, "POST");
    assert.equal(options.redirect, "error");
    assert.equal(options.credentials, "same-origin");
    assert.equal(options.headers["content-type"], "application/json");
    const request = JSON.parse(options.body);
    requests.push(request);
    return response(request);
  } });
  client.subscribe((record) => states.push(record.status));
  const params = { payload: { value: "original" } };
  const first = client.call("pool.execute", params);
  params.payload.value = "edited later";
  const records = await Promise.all([first, client.call("pool.execute", params)]);
  assert.equal(new Set(requests.map((request) => request.id)).size, 2);
  assert.equal(records[0].request.params.payload.value, "original");
  assert.equal(records[0].response.result.pid, 42);
  assert.deepEqual(states, ["pending", "pending", "success", "success"]);
});

test("HTTP, RPC, protocol, and network errors remain distinguishable", async () => {
  const cases = [
    { status: "rpc-error", fetchImpl: async (_, options) => response(JSON.parse(options.body), { error: { code: -32006, message: "not initialized", data: { kind: "NotInitialized" } } }) },
    { status: "http-error", fetchImpl: async () => new Response("bad gateway", { status: 502 }) },
    { status: "protocol-error", fetchImpl: async () => new Response("<html>not RPC</html>") },
    { status: "protocol-error", fetchImpl: async () => response({ id: "wrong-id" }) },
    { status: "protocol-error", fetchImpl: async (_, options) => response(JSON.parse(options.body), { error: null }) },
    { status: "transport-error", fetchImpl: async () => { throw new Error("offline"); } },
  ];
  for (const item of cases) {
    const record = await rpc.createClient(item).call("pool.stats", {});
    assert.equal(record.status, item.status);
    assert.ok(record.errorMessage);
    if (item.status === "http-error") assert.equal(record.responseText, "bad gateway");
    if (item.status === "rpc-error") assert.equal(record.response.error.data.kind, "NotInitialized");
  }
});

test("HTTP wait timeout warns about unknown execution outcome and does not retry", async () => {
  let calls = 0;
  const client = rpc.createClient({ fetchImpl: (_, { signal }) => {
    calls += 1;
    return new Promise((_, reject) => signal.addEventListener("abort", () => reject(new Error("aborted")), { once: true }));
  } });
  const record = await client.call("pool.execute", rpc.taskTemplate("sleep"), { waitTimeoutMs: 5 });
  assert.equal(calls, 1);
  assert.equal(record.status, "transport-error");
  assert.match(record.errorMessage, /任务可能仍在执行/);
});

// Controller tests use lightweight element doubles, not browser/visual QA.
class Element {
  constructor() { this.children = []; this.listeners = {}; this.value = ""; this.dataset = {}; this.attributes = {}; this.valid = true; }
  append(...nodes) { this.children.push(...nodes); }
  replaceChildren(...nodes) { this.children = nodes; }
  setAttribute(name, value) { this.attributes[name] = value; }
  set innerHTML(_) { throw new Error("Do not render RPC content through innerHTML"); }
  addEventListener(name, listener) { (this.listeners[name] ??= []).push(listener); }
  reportValidity() { return this.valid; }
  focus() { this.focused = true; }
  dispatch(name, properties = {}) { return Promise.all((this.listeners[name] || []).map((listener) => listener({ preventDefault() {}, ...properties }))); }
}

function setup(fetchImpl) {
  const html = readFileSync(path.join(__dirname, "../web/index.html"), "utf8");
  const elements = new Map([...html.matchAll(/id="([^"]+)"/g)].map(([, id]) => [id, new Element()]));
  const el = (id) => elements.get(`debug-${id}`);
  el("method").value = "pool.execute";
  el("example").value = "sum";
  el("count").value = "1";
  el("wait").value = "60";
  const sent = [];
  const client = rpc.createClient({ fetchImpl: async (url, options) => {
    const request = JSON.parse(options.body);
    sent.push(request);
    return fetchImpl ? fetchImpl(request) : response(request);
  } });
  const state = { busy: [], outcomes: null };
  const initParams = { core_pool_size: 3, maximum_pool_size: 5, keep_alive_time: 40, time_unit: "seconds",
    work_queue: { type: "bounded", capacity: 7 }, process_factory: "registered-worker", rejected_execution_handler: "abort" };
  const ui = mount({ client, readInitializationParams: () => initParams,
    onBusyChange: (value) => state.busy.push(value), onSettled: (records) => { state.outcomes = records; },
    document: { getElementById: (id) => { assert.ok(elements.has(id), `Missing ${id}`); return elements.get(id); }, createElement: () => new Element() },
  });
  return { el, sent, client, ui, state, initParams };
}

test("debug form validates JSON and concurrency before any network call", async () => {
  const { el, sent } = setup();
  el("params").value = "{";
  await el("form").dispatch("submit");
  assert.match(el("status").textContent, /JSON 格式错误/);
  assert.equal(el("status").dataset.error, "true");
  el("params").value = "{}";
  el("count").value = "17";
  await el("form").dispatch("submit");
  assert.equal(sent.length, 0);
  el("count").value = "1";
  el("form").valid = false;
  await el("form").dispatch("submit");
  assert.equal(sent.length, 0);
});

test("parallel task submission records each call and blocks accidental double submit", async () => {
  const pending = [];
  const { el, sent, state } = setup((request) => new Promise((resolve) => pending.push(() => resolve(response(request)))));
  el("count").value = "3";
  const submission = el("form").dispatch("submit");
  assert.equal(sent.length, 3);
  assert.equal(el("fields").disabled, true);
  assert.equal(el("clear").disabled, true);
  await el("form").dispatch("submit");
  assert.equal(sent.length, 3);
  await el("clear").dispatch("click");
  assert.equal(el("history").children.length, 3);
  pending.forEach((resolve) => resolve());
  await submission;
  assert.deepEqual(state.busy, [true, false]);
  assert.equal(state.outcomes.length, 3);
  assert.match(el("status").textContent, /成功 3/);
  assert.equal(el("fields").disabled, false);
  assert.equal(JSON.parse(el("response").textContent).result.pid, 42);
});

test("initialization template uses real form values; management requests are single calls", async () => {
  const { el, sent, initParams, ui } = setup();
  el("method").value = "pool.initialize";
  await el("method").dispatch("change");
  assert.deepEqual(JSON.parse(el("params").value), initParams);
  assert.equal(el("count-label").hidden, true);
  ui.setBusy(true);
  await el("form").dispatch("submit");
  assert.equal(sent.length, 0);
  ui.setBusy(false);
  el("count").value = "8";
  await el("form").dispatch("submit");
  assert.equal(sent.length, 1);
  assert.deepEqual(sent[0].params, initParams);
  el("method").value = "pool.prestart";
  await el("method").dispatch("change");
  assert.equal(el("params").value, "{}");
});

test("RPC failure is visible and shared-client calls also populate the bounded history", async () => {
  const { el, client, sent } = setup((request) => response(request, { error: { code: -32001, message: "<script>rejected</script>" } }));
  await el("form").dispatch("submit");
  assert.equal(el("status").dataset.error, "true");
  assert.match(el("response").textContent, /-32001/);
  assert.match(el("response").textContent, /<script>rejected<\/script>/);
  for (let i = 0; i < 31; i += 1) await client.call("pool.stats", {});
  assert.equal(el("history").children.length, 30);
  const before = sent.length;
  await el("history").children[0].children[5].children[0].dispatch("click");
  assert.equal(sent.length, before, "Viewing history must not resubmit tasks");
  await el("clear").dispatch("click");
  assert.equal(el("history").children.length, 1);
  assert.match(el("status").textContent, /不会影响进程池/);
});

test("every existing dashboard binding still points to an HTML element", () => {
  const html = readFileSync(path.join(__dirname, "../web/index.html"), "utf8");
  const source = readFileSync(path.join(__dirname, "../web/dashboard.js"), "utf8");
  for (const [, id] of source.matchAll(/byId\("([^"]+)"\)/g)) assert.ok(html.includes(`id="${id}"`), id);
  assert.ok(html.indexOf('/assets/rpc-client.js') < html.indexOf('/assets/debugger.js'));
  assert.ok(html.indexOf('/assets/debugger.js') < html.indexOf('/assets/dashboard.js'));
});

async function setupFullDashboard(initialHash = "") {
  const html = readFileSync(path.join(__dirname, "../web/index.html"), "utf8");
  const nodes = new Map([...html.matchAll(/id="([^"]+)"/g)].map(([, id]) => [id, new Element()]));
  for (const node of nodes.values()) {
    node.style = { setProperty() {} };
    node.classList = { add() {}, remove() {} };
    node.getBoundingClientRect = () => ({ width: 0, height: 0 });
  }
  const values = { core_pool_size: "1", maximum_pool_size: "3", keep_alive_time: "30", time_unit: "seconds",
    queue_capacity: "1", process_factory: "echo", rejected_execution_handler: "abort" };
  nodes.get("initialization-form").elements = { namedItem: (name) => ({ value: values[name] }) };
  for (const [key, value] of Object.entries({ method: "pool.execute", example: "sum", count: "1", wait: "60" })) nodes.get(`debug-${key}`).value = value;
  let snapshot = { initialized: false };
  let releaseTask;
  let poll;
  const sent = [];
  const doc = { getElementById: (id) => nodes.get(id), createElement: () => new Element() };
  const windowEvents = new Map();
  const browserWindow = { location: { host: "127.0.0.1:test", hash: initialHash },
    setInterval(fn) { poll = fn; }, addEventListener(name, listener) { windowEvents.set(name, listener); } };
  const context = vm.createContext({ document: doc, window: browserWindow,
    ResizeObserver: class { observe() {} }, AbortController, AbortSignal, TextEncoder, performance, setTimeout, clearTimeout,
    fetch: async (url, options) => {
      if (url === "/api/factories") return Response.json({ factories: ["echo"] });
      if (url === "/api/stats") return Response.json(snapshot);
      assert.equal(url, "/rpc");
      const request = JSON.parse(options.body);
      sent.push(request);
      if (request.method === "pool.initialize") {
        snapshot = { initialized: true, core_pool_size: 1, maximum_pool_size: 3, keep_alive_ms: 30_000,
          work_queue_capacity: 1, rejection_policy: "abort", worker_count: 0, busy_worker_count: 0,
          idle_worker_count: 0, queued_task_count: 0, completed_task_count: 0, failed_task_count: 0,
          rejected_task_count: 0, caller_runs_task_count: 0, workers: [] };
        return response(request, { result: snapshot });
      }
      snapshot.worker_count = 1;
      snapshot.busy_worker_count = 1;
      return new Promise((resolve) => { releaseTask = () => {
        snapshot.busy_worker_count = 0;
        snapshot.idle_worker_count = 1;
        snapshot.completed_task_count = 1;
        resolve(response(request));
      }; });
    },
  });
  for (const file of ["rpc-client.js", "debugger.js", "dashboard.js"]) {
    vm.runInContext(readFileSync(path.join(__dirname, "../web", file), "utf8"), context, { filename: file });
  }
  const flush = () => new Promise((resolve) => setImmediate(resolve));
  await flush();
  return { nodes, sent, flush, poll, releaseTask: () => releaseTask(), browserWindow, windowEvents };
}

test("tab switching preserves in-flight requests, input, history, and live polling", async () => {
  const { nodes, sent, flush, poll, releaseTask, browserWindow } = await setupFullDashboard();
  assert.equal(nodes.get("monitor-view").hidden, false);
  assert.equal(nodes.get("debug-view").hidden, true);
  assert.equal(nodes.get("monitor-empty").hidden, false);
  await nodes.get("open-debug-button").dispatch("click");
  assert.equal(browserWindow.location.hash, "#debug");
  assert.equal(nodes.get("monitor-view").hidden, true);
  assert.equal(nodes.get("debug-view").hidden, false);
  assert.equal(nodes.get("initialization-form").hidden, false);
  assert.equal(nodes.get("initialization-fields").disabled, false);
  await nodes.get("initialization-form").dispatch("submit");
  await flush();
  assert.equal(sent[0].method, "pool.initialize");
  assert.equal(sent[0].params.process_factory, "echo");
  assert.equal(nodes.get("initialization-form").hidden, true);
  assert.equal(nodes.get("runtime-panels").hidden, false);
  assert.equal(nodes.get("monitor-empty").hidden, true);
  assert.equal(nodes.get("monitor-view").hidden, true, "Initializing must not move the user away from debugging");
  assert.equal(nodes.get("debug-history").children.length, 1);

  const input = nodes.get("debug-params").value;
  const task = nodes.get("debug-form").dispatch("submit");
  assert.equal(nodes.get("prestart-button").disabled, true);
  await nodes.get("monitor-tab").dispatch("click");
  assert.equal(nodes.get("debug-view").hidden, true);
  assert.equal(nodes.get("monitor-view").hidden, false);
  await poll();
  assert.match(nodes.get("debug-pool-state").textContent, /忙碌 1/);
  releaseTask();
  await task;
  await flush();
  assert.equal(nodes.get("debug-view").hidden, true, "A completed request must not change the active tab");
  await nodes.get("debug-tab").dispatch("click");
  assert.equal(nodes.get("debug-view").hidden, false);
  assert.equal(nodes.get("debug-params").value, input);
  assert.equal(sent.length, 2, "Switching tabs must not reinitialize or resubmit");
  assert.equal(nodes.get("debug-history").children.length, 2);
  assert.equal(nodes.get("prestart-button").disabled, false);
  assert.match(nodes.get("debug-pool-state").textContent, /空闲 1/);
  assert.equal(JSON.parse(nodes.get("debug-response").textContent).result.pid, 42);
});

test("tab deep links, hash navigation, and keyboard selection stay synchronized", async () => {
  const { nodes, sent, browserWindow, windowEvents } = await setupFullDashboard("#debug");
  assert.equal(nodes.get("debug-view").hidden, false);
  assert.equal(nodes.get("debug-tab").attributes["aria-selected"], "true");
  assert.equal(nodes.get("debug-tab").tabIndex, 0);
  assert.equal(nodes.get("monitor-tab").tabIndex, -1);
  browserWindow.location.hash = "#monitor";
  windowEvents.get("hashchange")();
  assert.equal(nodes.get("monitor-view").hidden, false);
  assert.equal(nodes.get("debug-tab").attributes["aria-selected"], "false");
  await nodes.get("monitor-tab").dispatch("keydown", { key: "ArrowRight" });
  assert.equal(browserWindow.location.hash, "#debug");
  assert.equal(nodes.get("debug-tab").focused, true);
  await nodes.get("debug-tab").dispatch("keydown", { key: "Home" });
  assert.equal(browserWindow.location.hash, "#monitor");
  assert.equal(nodes.get("monitor-tab").focused, true);
  await nodes.get("monitor-tab").dispatch("keydown", { key: "End" });
  assert.equal(browserWindow.location.hash, "#debug");
  await nodes.get("debug-tab").dispatch("keydown", { key: "ArrowLeft" });
  assert.equal(browserWindow.location.hash, "#monitor");
  browserWindow.location.hash = "#unrecognized";
  windowEvents.get("hashchange")();
  assert.equal(nodes.get("monitor-view").hidden, false);
  assert.equal(nodes.get("debug-view").hidden, true);
  assert.equal(sent.length, 0);
});

test("HTML keeps all mutating controls in the debug panel and metrics in the monitor panel", () => {
  const html = readFileSync(path.join(__dirname, "../web/index.html"), "utf8");
  const stack = [];
  const ancestors = new Map();
  const voidTags = new Set(["meta", "link", "input", "br", "hr", "img"]);
  for (const match of html.matchAll(/<(\/?)([a-z][\w-]*)([^>]*)>/gi)) {
    const [, closing, tag, attributes] = match;
    if (closing) {
      assert.equal(stack.pop()?.tag, tag, `Unbalanced closing tag ${tag}`);
    } else {
      const id = attributes.match(/\bid="([^"]+)"/)?.[1];
      if (id) {
        assert.ok(!ancestors.has(id), `Duplicate id ${id}`);
        ancestors.set(id, stack.map((entry) => entry.id));
      }
      if (!voidTags.has(tag)) stack.push({ tag, id });
    }
  }
  assert.equal(stack.length, 0);
  for (const id of ["initialization-form", "prestart-button", "debug-form", "debug-history"]) {
    assert.ok(ancestors.get(id).includes("debug-view"));
    assert.ok(!ancestors.get(id).includes("monitor-view"));
  }
  for (const id of ["runtime-panels", "load-chart", "worker-rows"]) {
    assert.ok(ancestors.get(id).includes("monitor-view"));
    assert.ok(!ancestors.get(id).includes("debug-view"));
  }
});
