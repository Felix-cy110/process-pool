const test = require("node:test");
const assert = require("node:assert/strict");
const { readFileSync } = require("node:fs");
const path = require("node:path");
const { mount } = require("../web/agents.js");
class Element {
  constructor() { this.children = []; this.listeners = {}; this.value = ""; this.textContent = ""; this.dataset = {}; this.attributes = {}; }
  append(...nodes) { this.children.push(...nodes); }
  replaceChildren(...nodes) { this.children = nodes; }
  setAttribute(name, value) { this.attributes[name] = value; }
  set innerHTML(_) { throw new Error("Untrusted output must not use innerHTML"); }
  addEventListener(name, fn) { this.listeners[name] = fn; }
  reportValidity() { return true; }
  async dispatch(name) { return this.listeners[name]?.({ preventDefault() {} }); }
}
function setup(override) {
  const html = readFileSync(path.join(__dirname, "../web/index.html"), "utf8");
  const nodes = new Map([...html.matchAll(/id="(cc-[^"]+)"/g)].map(([, id]) => [id, new Element()]));
  const get = (id) => nodes.get(`cc-${id}`);
  const agents = ["a", "b"].map((id) => ({ id, label: id, generation: 1, pid: id === "a" ? 10 : 20, cwd: `/repo/${id}`, state: "idle", pending_permissions: {}, completed_turns: 0, failed_turns: 0 }));
  const requests = [];
  const status = { enabled: true, repository_ready: true, max_agents: 4, claude_program: "claude", agents };
  const handle = async (method, params) => {
    if (method === "cc.status") return status;
    if (method === "cc.get") return { agent: agents.find((a) => a.id === params.agent_id), events: [], cursor: 0 };
    return {};
  };
  const ui = mount({ document: { getElementById: (id) => { assert.ok(nodes.has(id), id); return nodes.get(id); }, createElement: () => new Element() }, schedule() {},
    client: { async call(method, params) {
      requests.push({ method, params });
      try { return { status: "success", response: { result: await (override || handle)(method, params, handle) } }; }
      catch (e) { return { status: "rpc-error", errorMessage: e.message }; }
    } },
  });
  const flush = () => new Promise((resolve) => setImmediate(resolve));
  const select = async (index = 0) => { await get("list").children[index].dispatch("click"); await flush(); };
  return { ui, get, agents, status, requests, flush, select };
}

test("CC only polls while selected, preserves per-Agent drafts, and never initializes the generic pool", async () => {
  const { ui, get, requests, flush, select } = setup();
  await ui.refresh(); assert.equal(requests.length, 0);
  ui.setActive(true); await flush(); await select();
  assert.equal(get("send").disabled, false);
  get("prompt").value = "draft A";
  await get("prompt").dispatch("input");
  await select(1);
  assert.equal(get("prompt").value, "");
  get("prompt").value = "draft B";
  await select(0);
  assert.equal(get("prompt").value, "draft A");
  ui.setActive(false);
  const count = requests.length;
  await ui.refresh();
  assert.equal(requests.length, count);
  assert.ok(requests.every((r) => r.method.startsWith("cc.")));
});

test("CC send validation and action guard prevent empty input and accidental double submission", async () => {
  let release;
  const { ui, get, requests, flush, select } = setup((method, params, handle) => method === "cc.send" ? new Promise((resolve) => { release = resolve; }) : handle(method, params));
  ui.setActive(true); await flush(); await select();
  await get("send-form").dispatch("submit");
  assert.match(get("message").textContent, /填写提示词/);
  get("prompt").value = "中".repeat(12000);
  await get("send-form").dispatch("submit");
  assert.match(get("message").textContent, /32 KiB/);
  get("prompt").value = "hello";
  const pending = get("send-form").dispatch("submit");
  assert.equal(get("send").disabled, true);
  await get("send-form").dispatch("submit");
  assert.equal(requests.filter((r) => r.method === "cc.send").length, 1);
  assert.deepEqual(requests.find((r) => r.method === "cc.send").params, { agent_id: "a", generation: 1, prompt: "hello" });
  release({}); await pending;
  assert.equal(get("prompt").value, "");
});

test("permission cards render text safely and send explicit one-time decisions, never modified tool input", async () => {
  const { ui, get, agents, requests, flush, select } = setup();
  agents[0].state = "awaiting_permission";
  agents[0].pending_permissions = { p1: { tool_name: "Bash", input: { command: "<script>not HTML</script>" } } };
  ui.setActive(true); await flush(); await select();
  const card = get("permissions").children[0];
  assert.match(card.children[1].textContent, /<script>/);
  assert.equal(get("send").disabled, true);
  await card.children[2].dispatch("click");
  assert.deepEqual(requests.find((r) => r.method === "cc.permission").params, { agent_id: "a", generation: 1, request_id: "p1", allow: false });
});

test("late selected-Agent responses cannot overwrite another Agent's session", async () => {
  let release;
  let block = false;
  const { ui, get, flush, select } = setup((method, params, handle) => {
    if (block && method === "cc.get" && params.agent_id === "a") return new Promise((resolve) => { release = () => handle(method, params).then(resolve); });
    return handle(method, params);
  });
  ui.setActive(true); await flush(); await select();
  block = true;
  const polling = ui.refresh(); await flush();
  await select(1);
  release(); await polling;
  assert.equal(get("title").textContent, "b");
  await ui.refresh();
  assert.equal(get("cwd").textContent, "/repo/b");
});

test("failed operations retain the prompt, show errors, and are not retried automatically", async () => {
  const { ui, get, requests, flush, select } = setup((method, params, handle) => {
    if (method === "cc.send") throw new Error("offline or authentication required");
    return handle(method, params);
  });
  ui.setActive(true); await flush(); await select();
  get("prompt").value = "keep this prompt";
  await get("send-form").dispatch("submit");
  assert.equal(get("prompt").value, "keep this prompt");
  assert.equal(get("message").dataset.error, "true");
  assert.match(get("message").textContent, /authentication/);
  await ui.refresh();
  assert.equal(requests.filter((r) => r.method === "cc.send").length, 1);
});
