(function (root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  else root.ProcessPoolRpc = api;
})(globalThis, () => {
  "use strict";

  const METHODS = ["pool.initialize", "pool.execute", "pool.prestart", "pool.stats"];
  const MAX_BATCH = 16;
  const MAX_REQUEST_BYTES = 64 * 1024;
  let sequence = 0;

  function parseParams(source) {
    let params;
    try { params = JSON.parse(source); }
    catch (error) { throw new Error(`JSON 格式错误：${error.message}`); }
    if (!params || Array.isArray(params) || typeof params !== "object") {
      throw new Error("params 必须是 JSON 对象，例如 {}。");
    }
    if (new TextEncoder().encode(source).length > MAX_REQUEST_BYTES) {
      throw new Error("调试请求参数不能超过 64 KiB。");
    }
    return params;
  }

  function validateCount(method, count) {
    if (!METHODS.includes(method)) throw new Error("不支持的调试接口。");
    if (!Number.isInteger(count) || count < 1 || count > MAX_BATCH) {
      throw new Error(`同时投放数量必须是 1–${MAX_BATCH} 的整数。`);
    }
    if (method !== "pool.execute" && count !== 1) {
      throw new Error("只有任务投放支持并发调用，管理接口一次只能调用一次。");
    }
  }

  function taskTemplate(example) {
    const payloads = {
      sum: { op: "sum", values: [7, 11, 24] },
      echo: { op: "echo", value: "来自 Web 调试器的任务" },
      sleep: { op: "sleep", millis: 1500, value: "观察忙碌进程和等待队列" },
      fail: { op: "fail" },
    };
    if (!Object.hasOwn(payloads, example)) throw new Error("未知任务示例。");
    return { payload: payloads[example], timeout_ms: 5000 };
  }

  function createClient({ fetchImpl = (...args) => fetch(...args), now = () => performance.now() } = {}) {
    const listeners = new Set();
    function emit(record) {
      for (const listener of listeners) listener(record);
    }

    async function call(method, params, { waitTimeoutMs = 60_000 } = {}) {
      validateCount(method, 1);
      if (!Number.isSafeInteger(waitTimeoutMs) || waitTimeoutMs < 1 || waitTimeoutMs > 300_000) {
        throw new Error("HTTP 等待上限必须在 1–300000 毫秒之间。");
      }
      const request = {
        jsonrpc: "2.0", id: `web-${Date.now()}-${++sequence}`, method,
        params: parseParams(JSON.stringify(params)),
      };
      const record = {
        request, startedAt: new Date(), status: "pending", httpStatus: null,
        durationMs: null, response: null, responseText: "", errorMessage: "",
      };
      const started = now();
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), waitTimeoutMs);
      emit(record);
      try {
        const response = await fetchImpl("/rpc", {
          method: "POST", headers: { "content-type": "application/json" },
          body: JSON.stringify(request), cache: "no-store", credentials: "same-origin",
          redirect: "error", signal: controller.signal,
        });
        record.httpStatus = response.status;
        record.responseText = await response.text();
        try { record.response = JSON.parse(record.responseText); }
        catch { /* Preserve non-JSON HTTP errors verbatim for diagnosis. */ }
        const message = record.response;
        const hasResult = message && Object.hasOwn(message, "result");
        const hasError = message && Object.hasOwn(message, "error");
        if (!response.ok) {
          record.status = "http-error";
          record.errorMessage = `HTTP ${response.status}，请查看响应正文。`;
        } else if (!message || message.jsonrpc !== "2.0" || message.id !== request.id ||
                   Boolean(hasResult) === Boolean(hasError) ||
                   (hasError && (!Number.isInteger(message.error?.code) || typeof message.error?.message !== "string"))) {
          record.status = "protocol-error";
          record.errorMessage = "响应不是与本次请求匹配的 JSON-RPC 消息，执行结果未确认，请先检查池状态。";
        } else if (hasError) {
          record.status = "rpc-error";
          record.errorMessage = `RPC ${message.error.code}：${message.error.message}`;
        } else {
          record.status = "success";
        }
      } catch (error) {
        record.status = "transport-error";
        record.errorMessage = controller.signal.aborted
          ? "HTTP 等待超时，任务可能仍在执行；请先查询状态，不要盲目重试。"
          : `网络请求失败：${error.message}。执行结果未确认，请先查询状态再决定是否重试。`;
      } finally {
        clearTimeout(timer);
        record.durationMs = Math.round(now() - started);
        emit(record);
      }
      return record;
    }

    return { call, subscribe(listener) { listeners.add(listener); return () => listeners.delete(listener); } };
  }

  return { createClient, parseParams, validateCount, taskTemplate, MAX_BATCH };
});
