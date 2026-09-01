/* Claude Code session controller. No command, cwd, environment or PID is accepted from the UI. */
(function (root, factory) {
  if (typeof module === "object" && module.exports) module.exports = factory();
  else root.ProcessPoolAgents = factory();
})(typeof globalThis !== "undefined" ? globalThis : this, function () {
  "use strict";
  const labels = { starting: "启动中", idle: "空闲", busy: "执行中", awaiting_permission: "等待授权", interrupting: "中断中", stopped: "已停止", failed: "已退出 / 失败" };
  const running = (state) => !["stopped", "failed"].includes(state);
  function mount({ document: doc = document, client, schedule = setInterval }) {
    const el = (id) => doc.getElementById(`cc-${id}`);
    let active = false, polling = false, acting = false, selected = null, inspected, revision = 0;
    let status = null, detail = null;
    const cursors = new Map(), logs = new Map(), drafts = new Map();
    const node = (tag, text, cls) => { const n = doc.createElement(tag); n.textContent = text; if (cls) n.className = cls; return n; };
    const report = (text, error = false) => { el("message").textContent = text; el("message").dataset.error = String(error); };
    async function call(method, params = {}, waitTimeoutMs = 15000) {
      const result = await client.call(method, params, { waitTimeoutMs });
      if (result.status !== "success") throw new Error(result.errorMessage);
      return result.response.result;
    }
    function controls() {
      el("prepare").disabled = acting || status?.enabled === false;
      el("create").disabled = acting || !status?.repository_ready || status.agents.filter((a) => running(a.state)).length >= status.max_agents;
      el("send").disabled = acting || detail?.state !== "idle";
      el("prompt").disabled = !selected;
      el("interrupt").disabled = acting || !["busy", "awaiting_permission"].includes(detail?.state);
      el("stop").disabled = acting || !detail || detail.state === "stopped";
      el("restart").disabled = acting || !detail || running(detail.state);
    }
    function taskDescription(agent) {
      if (!agent) return "将鼠标移到进程方块上查看";
      if (agent.current_task) return agent.current_task;
      return { idle: "空闲，等待下一轮任务复用", failed: agent.last_error || "进程异常退出", starting: "正在启动 Claude Code", stopped: "槽位已释放" }[agent.state]
        || labels[agent.state] || agent.state;
    }
    function inspect(agent) {
      el("inspector-handle").textContent = agent ? `${agent.id} · generation ${agent.generation} · PID ${agent.pid ?? "—"}` : "—";
      el("inspector-task").textContent = taskDescription(agent);
      el("inspector-reuse").textContent = agent
        ? `已完成 ${agent.completed_turns} 轮 · 失败 ${agent.failed_turns} 轮 · 累计复用 ${agent.completed_turns + agent.failed_turns} 次`
        : "—";
    }
    function releaseInspection(visible) {
      inspected = undefined;
      inspect((status?.agents || []).find((agent) => agent.id === selected) || visible[0] || null);
    }
    function renderMonitor() {
      const capacity = status?.max_agents || 0;
      const visible = (status?.agents || []).filter((agent) => agent.state !== "stopped");
      const agentsBySlot = new Map(visible.map((agent) => [agent.slot, agent]));
      const workingCount = visible.filter((agent) => ["starting", "busy", "awaiting_permission", "interrupting"].includes(agent.state)).length;
      const failedCount = visible.filter((agent) => agent.state === "failed").length;
      el("monitor-summary").textContent = capacity
        ? `${capacity} 槽 · ${visible.length - failedCount} 绿色占用 · ${workingCount} 正在工作 · ${failedCount} 出错`
        : "CC 管理未启用";
      el("slots").replaceChildren();
      for (let slot = 1; slot <= capacity; slot += 1) {
        const agent = agentsBySlot.get(slot);
        if (!agent) {
          const empty = node("div", "", "cc-process-slot cc-process-slot-empty");
          empty.setAttribute("aria-label", `槽位 ${slot}，未占用`);
          empty.append(node("strong", String(slot).padStart(2, "0")), node("span", "EMPTY"));
          empty.addEventListener("mouseenter", () => { inspected = null; inspect(null); });
          empty.addEventListener("mouseleave", () => releaseInspection(visible));
          el("slots").append(empty);
          continue;
        }
        const isFailed = agent.state === "failed";
        const isWorking = ["starting", "busy", "awaiting_permission", "interrupting"].includes(agent.state);
        const block = node("button", "", `cc-process-slot ${isFailed ? "cc-process-slot-failed" : "cc-process-slot-occupied"}${isWorking ? " is-working" : ""}`);
        block.type = "button";
        block.setAttribute("aria-label", `槽位 ${slot}，${agent.label}，${labels[agent.state] || agent.state}。唯一句柄 ${agent.id}。${taskDescription(agent)}`);
        block.setAttribute("aria-pressed", String(agent.id === selected));
        block.append(node("strong", String(slot).padStart(2, "0")), node("span", labels[agent.state] || agent.state), node("small", agent.id.slice(-8)));
        block.addEventListener("mouseenter", () => { inspected = agent.id; inspect(agent); });
        block.addEventListener("mouseleave", () => releaseInspection(visible));
        block.addEventListener("focus", () => { inspected = agent.id; inspect(agent); });
        block.addEventListener("blur", () => releaseInspection(visible));
        block.addEventListener("click", () => select(agent.id));
        el("slots").append(block);
      }
      inspect(inspected === null ? null : visible.find((agent) => agent.id === inspected)
        || (status?.agents || []).find((agent) => agent.id === selected) || visible[0] || null);
    }
    function renderList() {
      el("list").replaceChildren();
      for (const agent of status?.agents || []) {
        const button = node("button", "", "cc-agent-card");
        button.type = "button";
        button.setAttribute("aria-pressed", String(agent.id === selected));
        button.append(node("strong", agent.label), node("span", `${labels[agent.state] || agent.state} · PID ${agent.pid ?? "—"}`), node("small", agent.id));
        button.addEventListener("click", () => select(agent.id));
        el("list").append(button);
      }
      if (!status?.agents?.length) el("list").append(node("p", "尚无 Agent。准备项目后新建，才会启动本机 Claude Code。", "form-note"));
    }
    function renderDetail() {
      el("empty").hidden = Boolean(detail);
      el("session").hidden = !detail;
      if (!detail) { controls(); return; }
      el("title").textContent = detail.label;
      el("identity").textContent = `${labels[detail.state]} · PID ${detail.pid ?? "—"} · 第 ${detail.generation} 次启动 · 成功 ${detail.completed_turns} / 失败 ${detail.failed_turns} 轮`;
      el("cwd").textContent = detail.cwd;
      el("session-id").textContent = detail.session_id || "首轮对话后生成";
      el("last-error").textContent = detail.last_error || "";
      el("output").textContent = (logs.get(selected) || []).join("\n\n") || "等待对话输出…";
      el("permissions").replaceChildren();
      for (const [requestId, request] of Object.entries(detail.pending_permissions || {})) {
        const card = node("section", "", "cc-permission");
        card.append(node("strong", `工具权限请求 · ${request.tool_name}`), node("pre", JSON.stringify(request.input, null, 2)));
        for (const [allow, text] of [[false, "拒绝"], [true, "仅允许这一次"]]) {
          const button = node("button", text, `button ${allow ? "button-primary" : "button-ghost"}`);
          button.type = "button"; button.disabled = acting;
          const target = { agent_id: detail.id, generation: detail.generation, request_id: requestId, allow };
          button.addEventListener("click", () => action("cc.permission", target));
          card.append(button);
        }
        el("permissions").append(card);
      }
      controls();
    }
    function formatEvent(event) {
      const data = event.data;
      if (event.kind === "user") return `你：${data.text}`;
      if (event.kind === "lifecycle" || event.kind === "stderr") return `[${event.kind}] ${data.message || data.text}`;
      if (event.kind === "permission") return `[权限] ${data.tool_name}：${data.allow ? "允许一次" : "拒绝"}`;
      if (data.type === "assistant") return `Claude：${(data.message?.content || []).map((part) => part.text || (part.type === "tool_use" ? `[工具 ${part.name}] ${JSON.stringify(part.input)}` : "")).filter(Boolean).join("\n")}`;
      if (data.type === "result") return `[本轮${data.is_error ? "失败" : "结束"}] ${data.result || JSON.stringify(data.errors || data.subtype)}`;
      // Partial output is shown in a separate live line; keep the event log readable.
      if (["stream_event", "control_response", "control_request", "control_cancel_request"].includes(data.type)) return null;
      if (data.type === "system") return `[系统] ${data.subtype || ""}`;
      return JSON.stringify(data);
    }
    async function refresh() {
      if (!active || polling || acting) return;
      polling = true;
      const version = revision, id = selected;
      try {
        const next = await call("cc.status");
        if (version !== revision) return;
        status = next;
        el("environment").textContent = next.enabled
          ? `${next.repository_ready ? "项目已就绪" : "项目未准备"} · 本机命令 ${next.claude_program} · 同时最多 ${next.max_agents} 个 CC 进程`
          : next.reason;
        el("repository").textContent = next.repository_path || "";
        renderMonitor(); renderList(); controls();
        if (id) {
          const result = await call("cc.get", { agent_id: id, after_event_id: cursors.get(id) || 0 });
          if (version !== revision || id !== selected) return;
          detail = result.agent;
          const output = logs.get(id) || [];
          if (result.truncated) output.push("[提示] 较早事件已超出服务端缓存，以下仅为最近输出。");
          for (const event of result.events) {
            const text = formatEvent(event); if (text) output.push(text);
            const delta = event.data?.event?.delta;
            if (event.data?.type === "stream_event" && delta?.text) el("stream").textContent = (el("stream").textContent + delta.text).slice(-8192);
            if (event.data?.type === "assistant" || event.data?.type === "result") el("stream").textContent = "";
          }
          logs.set(id, output.slice(-120)); cursors.set(id, result.cursor);
          renderDetail();
        }
      } catch (error) { if (version === revision) { detail = null; renderDetail(); report(`无法确认 CC 状态：${error.message}`, true); } }
      finally { polling = false; }
    }
    function select(id) {
      if (selected) drafts.set(selected, el("prompt").value);
      selected = id; revision += 1;
      inspected = id;
      detail = status?.agents.find((agent) => agent.id === id) || null;
      el("prompt").value = drafts.get(id) || ""; el("stream").textContent = "";
      renderMonitor(); renderList(); renderDetail();
      void refresh();
    }
    async function action(method, params, success) {
      if (acting) return;
      acting = true; revision += 1; controls(); renderDetail();
      report(method === "cc.prepare" ? "正在检查本机 Claude Code 并准备 conduit，请稍候…" : "正在提交操作…");
      try {
        const result = await call(method, params, method === "cc.prepare" ? 140000 : 20000);
        success?.(result);
        report(method === "cc.prepare" ? `项目已准备 · ${result.claude_version}` : "操作已提交；进程状态会自动刷新。不会自动重试。");
      } catch (error) { report(error.message, true); }
      finally { acting = false; controls(); await refresh(); }
    }
    el("prepare").addEventListener("click", () => action("cc.prepare", {}));
    el("create-form").addEventListener("submit", (event) => {
      event.preventDefault();
      if (!el("create-form").reportValidity()) return;
      return action("cc.create", { label: el("label").value }, (agent) => {
        selected = agent.id; detail = agent; revision += 1; el("prompt").value = ""; el("stream").textContent = ""; renderMonitor(); renderDetail();
      });
    });
    el("prompt").addEventListener("input", () => { if (selected) drafts.set(selected, el("prompt").value); });
    el("send-form").addEventListener("submit", (event) => {
      event.preventDefault();
      const prompt = el("prompt").value, id = selected;
      if (!prompt.trim()) { report("请先填写提示词。", true); return; }
      if (new TextEncoder().encode(prompt).length > 32768) { report("提示词不能超过 32 KiB。", true); return; }
      if (detail?.state !== "idle") return;
      return action("cc.send", { agent_id: id, generation: detail.generation, prompt }, () => {
        drafts.set(id, ""); if (selected === id && el("prompt").value === prompt) el("prompt").value = "";
      });
    });
    for (const method of ["interrupt", "stop", "restart"]) el(method).addEventListener("click", () => {
      if (!detail) return;
      return action(`cc.${method}`, { agent_id: detail.id, generation: detail.generation });
    });
    controls(); renderMonitor(); renderDetail();
    schedule(() => { void refresh(); }, 1000);
    return { setActive(value) { active = value; if (active) void refresh(); }, refresh };
  }
  return { mount, labels };
});
