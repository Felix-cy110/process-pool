(function (root, factory) {
  const api = factory(typeof module === "object" && module.exports ? require("./rpc-client.js") : root.ProcessPoolRpc);
  if (typeof module === "object" && module.exports) module.exports = api;
  else root.ProcessPoolDebugger = api;
})(globalThis, (rpc) => {
  "use strict";

  const statusLabel = (record) => ({
    pending: "执行中", success: "成功", "rpc-error": `RPC ${record.response?.error?.code}`,
    "http-error": `HTTP ${record.httpStatus}`, "protocol-error": "协议错误", "transport-error": "结果未确认",
  })[record.status];

  function mount({ client, readInitializationParams, onSettled, onBusyChange, document: doc = document }) {
    const ids = ["form", "fields", "method", "example", "example-label", "hint", "params", "template", "format",
      "count", "count-label", "wait", "send", "status", "response", "request", "result-meta", "history", "clear"];
    const el = Object.fromEntries(ids.map((id) => [id, doc.getElementById(`debug-${id}`)]));
    const records = [];
    let selectedId = null;
    let busy = false;
    let externallyBusy = false;

    function message(text, isError = false) {
      el.status.textContent = text;
      el.status.dataset.error = String(isError);
    }

    function updateControls() {
      el.fields.disabled = busy || externallyBusy;
      el.clear.disabled = records.some((record) => record.status === "pending");
    }

    function select(record) {
      selectedId = record.request.id;
      el.request.textContent = JSON.stringify(record.request, null, 2);
      el.response.textContent = record.status === "pending" ? "请求已发出，等待响应…"
        : record.response !== null ? JSON.stringify(record.response, null, 2)
        : record.responseText || record.errorMessage;
      el["result-meta"].textContent = `${record.request.method} · ${statusLabel(record)} · ${record.httpStatus === null ? "HTTP —" : `HTTP ${record.httpStatus}`} · ${record.durationMs ?? "—"} ms`;
      if (!busy) message(record.errorMessage || (record.status === "pending" ? "请求已发出，等待响应。" : "调用成功，可在下方观察进程池状态。"), !["pending", "success"].includes(record.status));
      renderHistory();
    }

    function renderHistory() {
      el.history.replaceChildren();
      if (!records.length) {
        const row = doc.createElement("tr");
        const cell = doc.createElement("td");
        cell.colSpan = 6;
        cell.className = "empty-row";
        cell.textContent = "尚无调用。初始化、预热和调试请求都会记录在这里。";
        row.append(cell);
        el.history.append(row);
      }
      for (const record of records) {
        const row = doc.createElement("tr");
        row.setAttribute("aria-selected", String(record.request.id === selectedId));
        const values = [record.startedAt.toLocaleTimeString("zh-CN", { hour12: false }), record.request.method,
          statusLabel(record), record.durationMs === null ? "—" : `${record.durationMs} ms`, record.response?.result?.pid ?? "—"];
        values.forEach((value, index) => {
          const cell = doc.createElement("td");
          cell.textContent = value;
          if (index === 2) cell.className = `debug-record-${record.status === "success" ? "success" : record.status === "pending" ? "pending" : "error"}`;
          row.append(cell);
        });
        const cell = doc.createElement("td");
        const button = doc.createElement("button");
        button.type = "button";
        button.className = "button button-ghost button-small";
        button.textContent = "查看";
        button.setAttribute("aria-label", `查看请求 ${record.request.id}`);
        button.addEventListener("click", () => select(record));
        cell.append(button);
        row.append(cell);
        el.history.append(row);
      }
      updateControls();
    }

    client.subscribe((record) => {
      if (!records.some((item) => item.request.id === record.request.id)) {
        records.unshift(record);
        records.splice(30);
        selectedId = record.request.id;
      }
      if (selectedId === record.request.id) select(record);
      else renderHistory();
    });

    function loadTemplate() {
      const method = el.method.value;
      const isTask = method === "pool.execute";
      el["example-label"].hidden = !isTask;
      el["count-label"].hidden = !isTask;
      el.count.disabled = !isTask;
      if (!isTask) el.count.value = "1";
      el.hint.textContent = {
        "pool.execute": "投放真实任务，worker 由池按需创建。内置示例仅适用于 echo worker；自定义 worker 请修改 payload。timeout_ms 是任务执行超时，不包含排队时间。",
        "pool.initialize": "模板取自上方七参数表单（包括已注册工厂名称）。这里只填 params，不要粘贴整个 RPC 信封；已初始化的池不能再次初始化。",
        "pool.prestart": "无需参数，使用 {}。创建缺少的核心进程，不突破核心数；核心数为 0 时不会创建进程。",
        "pool.stats": "无需参数，使用 {}。查询当前配置、worker 和任务统计；不会创建进程。",
      }[method];
      const params = isTask ? rpc.taskTemplate(el.example.value)
        : method === "pool.initialize" ? readInitializationParams() : {};
      el.params.value = JSON.stringify(params, null, 2);
    }

    el.method.addEventListener("change", loadTemplate);
    el.example.addEventListener("change", loadTemplate);
    el.template.addEventListener("click", loadTemplate);
    el.format.addEventListener("click", () => {
      try { el.params.value = JSON.stringify(rpc.parseParams(el.params.value), null, 2); }
      catch (error) { message(error.message, true); }
    });
    el.form.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (busy || externallyBusy || !el.form.reportValidity()) return;
      const method = el.method.value;
      let params;
      const count = method === "pool.execute" ? Number(el.count.value) : 1;
      const waitTimeoutMs = Number(el.wait.value) * 1000;
      try {
        params = rpc.parseParams(el.params.value);
        rpc.validateCount(method, count);
        if (!Number.isInteger(waitTimeoutMs / 1000) || waitTimeoutMs < 1000 || waitTimeoutMs > 300_000) throw new Error("HTTP 等待上限必须是 1–300 的整数秒。");
      } catch (error) { message(error.message, true); return; }
      busy = true;
      onBusyChange(true);
      updateControls();
      message(`正在发送 ${count} 个真实请求…`);
      let completed = 0;
      try {
        const outcomes = await Promise.all(Array.from({ length: count }, async () => {
          const record = await client.call(method, params, { waitTimeoutMs });
          completed += 1;
          el.send.textContent = `执行中 ${completed} / ${count}`;
          return record;
        }));
        const succeeded = outcomes.filter((record) => record.status === "success").length;
        const unknown = outcomes.filter((record) => ["transport-error", "protocol-error"].includes(record.status)).length;
        message(`已完成 ${count} 次调用：成功 ${succeeded}，失败或未确认 ${count - succeeded}。${unknown ? "部分执行结果未确认，请先查询状态，不要盲目重试。" : "点击记录可查看每条响应。"}`, succeeded !== count);
        onSettled(outcomes);
      } catch (error) { message(error.message, true); }
      finally {
        busy = false;
        onBusyChange(false);
        el.send.textContent = "发送请求";
        updateControls();
      }
    });
    el.clear.addEventListener("click", () => {
      if (records.some((record) => record.status === "pending")) return;
      records.length = 0;
      selectedId = null;
      el.request.textContent = "尚未发送请求。";
      el.response.textContent = "发送请求后，查看完整结果或错误详情。";
      el["result-meta"].textContent = "尚无调用";
      message("已清空页面记录；不会影响进程池或取消任务。");
      renderHistory();
    });

    loadTemplate();
    updateControls();
    message("就绪。先初始化进程池，再投放任务；查询状态无需初始化。");
    return { setBusy(value) { externallyBusy = value; updateControls(); } };
  }

  return { mount };
});
