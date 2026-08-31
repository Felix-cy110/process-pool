(() => {
  "use strict";

  const MAX_SAMPLES = 60;
  const history = [];
  const events = [];
  let previous = null;
  let previousAt = null;
  let paused = false;
  let refreshing = false;
  let initialized = false;
  let factoriesReady = false;
  let changingPool = false;
  let debugging = false;
  let debuggerUi = null;
  let agentsUi = null;
  let stateGeneration = 0;
  const rpcClient = ProcessPoolRpc.createClient();

  const byId = (id) => document.getElementById(id);
  const elements = {
    connection: byId("connection"),
    connectionLabel: byId("connection-label"),
    connectionEndpoint: byId("connection-endpoint"),
    pauseButton: byId("pause-button"),
    refreshButton: byId("refresh-button"),
    usageRing: byId("usage-ring"),
    usageValue: byId("usage-value"),
    workerRatio: byId("worker-ratio"),
    busyCount: byId("busy-count"),
    idleCount: byId("idle-count"),
    queueValue: byId("queue-value"),
    queueMeter: byId("queue-meter"),
    queueCaption: byId("queue-caption"),
    completedValue: byId("completed-value"),
    successRate: byId("success-rate"),
    failedValue: byId("failed-value"),
    failureCaption: byId("failure-caption"),
    rejectedValue: byId("rejected-value"),
    callerRunsCaption: byId("caller-runs-caption"),
    throughput: byId("throughput"),
    sampleCount: byId("sample-count"),
    chart: byId("load-chart"),
    chartEmpty: byId("chart-empty"),
    workerRows: byId("worker-rows"),
    workersBadge: byId("workers-badge"),
    configCore: byId("config-core"),
    configMax: byId("config-max"),
    configQueue: byId("config-queue"),
    configKeepalive: byId("config-keepalive"),
    configPolicy: byId("config-policy"),
    latency: byId("latency"),
    events: byId("events"),
    updatedAt: byId("updated-at"),
    initializationForm: byId("initialization-form"),
    initializationFields: byId("initialization-fields"),
    initializationStatus: byId("initialization-status"),
    initializationMessage: byId("initialization-message"),
    initializeButton: byId("initialize-button"),
    factorySelect: byId("factory-select"),
    factoryStatus: byId("factory-status"),
    runtimePanels: byId("runtime-panels"),
    prestartButton: byId("prestart-button"),
    debugPoolState: byId("debug-pool-state"),
    monitorTab: byId("monitor-tab"),
    debugTab: byId("debug-tab"),
    monitorView: byId("monitor-view"),
    debugView: byId("debug-view"),
    monitorEmpty: byId("monitor-empty"),
    monitorEmptyMessage: byId("monitor-empty-message"),
    openDebugButton: byId("open-debug-button"),
    pageTitle: byId("page-title"),
    pageDescription: byId("page-description"),
  };

  function showTab(name, { focus = false, syncHash = true } = {}) {
    if (!["monitor", "debug", "agents"].includes(name)) name = "monitor";
    for (const key of ["monitor", "debug", "agents"]) {
      const selected = key === name;
      byId(`${key}-view`).hidden = !selected;
      byId(`${key}-tab`).setAttribute("aria-selected", String(selected));
      byId(`${key}-tab`).tabIndex = selected ? 0 : -1;
    }
    elements.pageTitle.textContent = { monitor: "进程池运行监控", debug: "进程池接口调试", agents: "Agent 调试 · Claude Code" }[name];
    elements.pageDescription.textContent = {
      monitor: "每秒采集 worker、队列和任务执行指标。初始化与任务投放请切换至接口调试。",
      debug: "在此初始化、预热和投放任务。切换回运行监控不会中断请求，也不会清空调试记录。",
      agents: "在独立的 conduit 工作副本中管理本机 Claude Code。逐进程对话、观察输出与控制生命周期。",
    }[name];
    agentsUi?.setActive(name === "agents");
    elements.pauseButton.hidden = name === "agents";
    if (focus) byId(`${name}-tab`).focus();
    if (name === "monitor") drawChart();
    const hash = `#${name}`;
    if (syncHash && window.location.hash !== hash) window.location.hash = hash;
  }

  const tabNames = ["monitor", "debug", "agents"];
  tabNames.forEach((name, index) => {
    const tab = byId(`${name}-tab`);
    tab.addEventListener("click", () => showTab(name));
    tab.addEventListener("keydown", (event) => {
      let target;
      if (event.key === "ArrowLeft") target = (index + tabNames.length - 1) % tabNames.length;
      else if (event.key === "ArrowRight") target = (index + 1) % tabNames.length;
      else if (event.key === "Home") target = 0;
      else if (event.key === "End") target = tabNames.length - 1;
      else return;
      event.preventDefault();
      showTab(tabNames[target], { focus: true });
    });
  });
  elements.openDebugButton.addEventListener("click", () => showTab("debug", { focus: true }));
  window.addEventListener("hashchange", () => showTab(window.location.hash.slice(1), { syncHash: false }));

  function showLifecycle(isInitialized) {
    initialized = isInitialized;
    elements.initializationForm.hidden = initialized;
    elements.runtimePanels.hidden = !initialized;
    elements.monitorEmpty.hidden = initialized;
    if (!initialized) elements.monitorEmptyMessage.textContent = "服务在线，但尚未初始化进程池。请前往接口调试填写七个参数；初始化后，这里将显示运行状态。";
    elements.initializationFields.disabled = !factoriesReady || changingPool || debugging || initialized;
    elements.prestartButton.disabled = !initialized || changingPool || debugging;
    debuggerUi?.setBusy(changingPool);
    if (!initialized) elements.debugPoolState.textContent = "尚未初始化 · worker 0 · 请先提交七参数配置。";
    elements.initializationStatus.textContent = initialized
      ? "已初始化 · worker 按任务需求创建。若需提前启动至核心数量，可点击「预热核心进程」。更换配置需重启服务。"
      : "服务在线 · 等待使用方传入 7 个初始化参数，尚未创建 worker。";
  }

  function showActionMessage(message, isError = false) {
    elements.initializationMessage.textContent = message;
    elements.initializationMessage.dataset.error = String(isError);
  }

  async function loadFactories() {
    try {
      const response = await fetch("/api/factories", { cache: "no-store", signal: AbortSignal.timeout(5000) });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const { factories } = await response.json();
      elements.factorySelect.replaceChildren();
      factories.forEach((name) => {
        const option = document.createElement("option");
        option.value = name;
        option.textContent = name;
        elements.factorySelect.append(option);
      });
      factoriesReady = factories.length > 0;
      elements.initializationFields.disabled = !factoriesReady || changingPool || debugging || initialized;
      elements.factoryStatus.textContent = factoriesReady
        ? "工厂已加载，请选择要复用的 worker 程序。"
        : "服务端未登记进程工厂，请先配置 --factories 并重启服务。";
    } catch (error) {
      elements.factoryStatus.textContent = `无法加载进程工厂：${error.message}。请确认服务已更新；点击立即刷新可重试。`;
    }
  }

  async function callRpc(method, params) {
    const record = await rpcClient.call(method, params);
    if (record.status !== "success") throw new Error(record.errorMessage);
    return record.response.result;
  }

  async function changePool(method, params) {
    if (changingPool || debugging) return;
    changingPool = true;
    stateGeneration += 1;
    showLifecycle(initialized);
    showActionMessage("正在提交…");
    try {
      const result = await callRpc(method, params);
      if (method === "pool.initialize") {
        showLifecycle(true);
        showActionMessage("初始化成功，worker 仍为 0；提交任务时才会启动。");
      } else {
        showActionMessage(`预热完成，本次新增 ${result.started_worker_count} 个 worker。`);
      }
    } catch (error) {
      showActionMessage(`操作失败：${error.message}`, true);
    } finally {
      changingPool = false;
      stateGeneration += 1;
      paused = false;
      elements.pauseButton.textContent = "暂停刷新";
      showLifecycle(initialized);
      refresh();
    }
  }

  function readInitializationParams() {
    // Read controls directly so the template also works when the form is disabled.
    const value = (name) => elements.initializationForm.elements.namedItem(name).value;
    return {
      core_pool_size: Number(value("core_pool_size")),
      maximum_pool_size: Number(value("maximum_pool_size")),
      keep_alive_time: Number(value("keep_alive_time")),
      time_unit: value("time_unit"),
      work_queue: { type: "bounded", capacity: Number(value("queue_capacity")) },
      process_factory: value("process_factory"),
      rejected_execution_handler: value("rejected_execution_handler"),
    };
  }

  function number(value) {
    return new Intl.NumberFormat("zh-CN").format(value ?? 0);
  }

  function duration(milliseconds) {
    if (milliseconds === null || milliseconds === undefined) return "—";
    if (milliseconds < 1000) return `${milliseconds} ms`;
    if (milliseconds < 60_000) return `${(milliseconds / 1000).toFixed(milliseconds < 10_000 ? 1 : 0)} s`;
    if (milliseconds < 3_600_000) return `${Math.floor(milliseconds / 60_000)}m ${Math.floor((milliseconds % 60_000) / 1000)}s`;
    return `${Math.floor(milliseconds / 3_600_000)}h ${Math.floor((milliseconds % 3_600_000) / 60_000)}m`;
  }

  function policyLabel(value) {
    const labels = {
      abort: "Abort / 立即拒绝",
      discard: "Discard / 丢弃新任务",
      discard_oldest: "Discard Oldest",
      caller_runs: "Caller Runs",
    };
    return labels[value] || value || "—";
  }

  function setConnection(mode, label) {
    elements.connection.classList.remove("online", "offline", "paused");
    elements.connection.classList.add(mode);
    elements.connectionLabel.textContent = label;
  }

  function addEvent(message, level = "info") {
    events.unshift({ message, level, time: new Date() });
    events.splice(6);
    renderEvents();
  }

  function renderEvents() {
    elements.events.replaceChildren();
    events.forEach((event) => {
      const item = document.createElement("li");
      const dot = document.createElement("i");
      dot.className = `event-${event.level}`;
      const text = document.createElement("span");
      text.textContent = event.message;
      const time = document.createElement("time");
      time.textContent = event.time.toLocaleTimeString("zh-CN", { hour12: false });
      item.append(dot, text, time);
      elements.events.append(item);
    });
  }

  function detectEvents(stats) {
    if (!previous) {
      addEvent(`已连接，发现 ${stats.worker_count} 个 worker`, "good");
      return;
    }
    if (stats.worker_count > previous.worker_count) {
      addEvent(`进程池扩容至 ${stats.worker_count} 个 worker`, "good");
    } else if (stats.worker_count < previous.worker_count) {
      addEvent(`进程池回收至 ${stats.worker_count} 个 worker`, "info");
    }
    if (stats.failed_task_count > previous.failed_task_count) {
      addEvent(`新增 ${stats.failed_task_count - previous.failed_task_count} 个失败任务`, "bad");
    }
    if (stats.rejected_task_count > previous.rejected_task_count) {
      addEvent(`新增 ${stats.rejected_task_count - previous.rejected_task_count} 个拒绝任务`, "warn");
    }
    if (stats.queued_task_count > previous.queued_task_count && stats.queued_task_count > 0) {
      addEvent(`队列增加至 ${stats.queued_task_count} 个任务`, "warn");
    }
  }

  function renderWorkers(workers) {
    elements.workerRows.replaceChildren();
    if (!workers.length) {
      const row = document.createElement("tr");
      const cell = document.createElement("td");
      cell.colSpan = 7;
      cell.className = "empty-row";
      cell.textContent = "当前没有运行中的 worker";
      row.append(cell);
      elements.workerRows.append(row);
      return;
    }

    workers.forEach((worker) => {
      const row = document.createElement("tr");
      const values = [
        { text: `worker-${worker.worker_id}`, className: "worker-name" },
        { text: worker.process_id ?? "—" },
        { state: worker.state },
        { text: worker.current_task_id ? `#${worker.current_task_id}` : "—" },
        { text: duration(worker.state_for_ms) },
        { text: number(worker.handled_task_count) },
        { text: duration(worker.last_task_duration_ms) },
      ];
      values.forEach((value) => {
        const cell = document.createElement("td");
        if (value.state) {
          const pill = document.createElement("span");
          pill.className = `state-pill state-${value.state}`;
          pill.textContent = value.state === "busy" ? "忙碌" : "空闲";
          cell.append(pill);
        } else {
          cell.textContent = value.text;
          if (value.className) cell.className = value.className;
        }
        row.append(cell);
      });
      elements.workerRows.append(row);
    });
  }

  function renderStats(stats, elapsedSeconds) {
    const capacityUsage = stats.maximum_pool_size > 0 ? (stats.busy_worker_count / stats.maximum_pool_size) * 100 : 0;
    const queueUsage = stats.work_queue_capacity > 0 ? (stats.queued_task_count / stats.work_queue_capacity) * 100 : 0;
    const resolved = stats.completed_task_count + stats.failed_task_count;
    const successRate = resolved > 0 ? (stats.completed_task_count / resolved) * 100 : 100;
    const previousResolved = previous ? previous.completed_task_count + previous.failed_task_count : resolved;
    const rate = elapsedSeconds > 0 ? Math.max(0, resolved - previousResolved) / elapsedSeconds : 0;

    elements.usageValue.textContent = `${Math.round(capacityUsage)}%`;
    elements.usageRing.style.setProperty("--usage-deg", `${Math.min(100, capacityUsage) * 3.6}deg`);
    elements.workerRatio.textContent = `${stats.worker_count} / ${stats.maximum_pool_size}`;
    elements.busyCount.textContent = number(stats.busy_worker_count);
    elements.idleCount.textContent = number(stats.idle_worker_count);
    elements.queueValue.textContent = number(stats.queued_task_count);
    elements.queueMeter.style.width = `${Math.min(100, queueUsage)}%`;
    elements.queueCaption.textContent = `容量 ${number(stats.work_queue_capacity)} · ${Math.round(queueUsage)}%`;
    elements.completedValue.textContent = number(stats.completed_task_count);
    elements.successRate.textContent = `成功率 ${successRate.toFixed(1)}%`;
    elements.failedValue.textContent = number(stats.failed_task_count);
    elements.failureCaption.textContent = stats.failed_task_count === 0 ? "运行正常" : `错误占比 ${(100 - successRate).toFixed(1)}%`;
    elements.rejectedValue.textContent = number(stats.rejected_task_count);
    elements.callerRunsCaption.textContent = `Caller Runs ${number(stats.caller_runs_task_count)}`;
    elements.throughput.textContent = `${rate.toFixed(2)} task/s`;
    elements.workersBadge.textContent = `${stats.worker_count} 个进程`;
    elements.configCore.textContent = number(stats.core_pool_size);
    elements.configMax.textContent = number(stats.maximum_pool_size);
    elements.configQueue.textContent = number(stats.work_queue_capacity);
    elements.configKeepalive.textContent = duration(stats.keep_alive_ms);
    elements.configPolicy.textContent = policyLabel(stats.rejection_policy);
    elements.debugPoolState.textContent = `当前 worker ${stats.worker_count} / ${stats.maximum_pool_size} · 忙碌 ${stats.busy_worker_count} · 空闲 ${stats.idle_worker_count} · 排队 ${stats.queued_task_count} · 已拒绝 ${stats.rejected_task_count}`;
    renderWorkers(stats.workers || []);
  }

  function addSample(stats) {
    history.push({ busy: stats.busy_worker_count, queue: stats.queued_task_count });
    if (history.length > MAX_SAMPLES) history.shift();
    elements.sampleCount.textContent = `${history.length} / ${MAX_SAMPLES} 个采样点`;
    elements.chartEmpty.hidden = history.length >= 2;
    drawChart();
  }

  function drawChart() {
    const canvas = elements.chart;
    const rect = canvas.getBoundingClientRect();
    if (!rect.width || !rect.height) return;
    const scale = window.devicePixelRatio || 1;
    canvas.width = Math.round(rect.width * scale);
    canvas.height = Math.round(rect.height * scale);
    const context = canvas.getContext("2d");
    context.scale(scale, scale);
    context.clearRect(0, 0, rect.width, rect.height);

    const padding = { top: 12, right: 8, bottom: 12, left: 30 };
    const width = rect.width - padding.left - padding.right;
    const height = rect.height - padding.top - padding.bottom;
    const maxValue = Math.max(1, ...history.flatMap((point) => [point.busy, point.queue]));

    context.strokeStyle = "rgba(145, 174, 205, 0.10)";
    context.fillStyle = "#526174";
    context.font = "9px ui-monospace, SFMono-Regular, Menlo, monospace";
    context.textAlign = "right";
    for (let index = 0; index <= 4; index += 1) {
      const y = padding.top + (height * index) / 4;
      context.beginPath();
      context.moveTo(padding.left, y);
      context.lineTo(rect.width - padding.right, y);
      context.stroke();
      context.fillText(String(Math.round(maxValue * (1 - index / 4))), padding.left - 8, y + 3);
    }

    if (history.length < 2) return;
    const drawSeries = (key, color, fill) => {
      context.beginPath();
      history.forEach((point, index) => {
        const x = padding.left + (width * index) / Math.max(MAX_SAMPLES - 1, history.length - 1);
        const y = padding.top + height - (point[key] / maxValue) * height;
        if (index === 0) context.moveTo(x, y);
        else context.lineTo(x, y);
      });
      context.lineWidth = 2;
      context.strokeStyle = color;
      context.lineJoin = "round";
      context.lineCap = "round";
      context.stroke();

      if (fill) {
        const lastX = padding.left + (width * (history.length - 1)) / Math.max(MAX_SAMPLES - 1, history.length - 1);
        context.lineTo(lastX, padding.top + height);
        context.lineTo(padding.left, padding.top + height);
        context.closePath();
        const gradient = context.createLinearGradient(0, padding.top, 0, padding.top + height);
        gradient.addColorStop(0, fill);
        gradient.addColorStop(1, "rgba(187, 244, 81, 0)");
        context.fillStyle = gradient;
        context.fill();
      }
    };

    drawSeries("busy", "#bbf451", "rgba(187, 244, 81, 0.14)");
    drawSeries("queue", "#9c8cff");
  }

  async function refresh() {
    if (refreshing || paused || changingPool) return;
    refreshing = true;
    const generation = stateGeneration;
    const startedAt = performance.now();
    try {
      const response = await fetch("/api/stats", { cache: "no-store", signal: AbortSignal.timeout(5000) });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const stats = await response.json();
      if (generation !== stateGeneration) return;
      if (typeof stats.initialized !== "boolean") throw new Error("服务版本较旧，请重新启动服务");
      showLifecycle(stats.initialized);
      if (!stats.initialized) {
        previous = null;
        previousAt = null;
        history.length = 0;
        setConnection("online", "等待初始化");
        elements.updatedAt.textContent = "服务在线 · 尚未初始化";
        return;
      }
      const now = Date.now();
      const elapsedSeconds = previousAt ? (now - previousAt) / 1000 : 0;
      detectEvents(stats);
      renderStats(stats, elapsedSeconds);
      addSample(stats);
      previous = stats;
      previousAt = now;
      const latency = Math.round(performance.now() - startedAt);
      elements.latency.textContent = `${latency} ms`;
      elements.updatedAt.textContent = `最后刷新 ${new Date().toLocaleTimeString("zh-CN", { hour12: false })}`;
      setConnection(paused ? "paused" : "online", paused ? "刷新已暂停" : "实时连接");
    } catch (error) {
      if (generation !== stateGeneration) return;
      setConnection("offline", "连接中断");
      elements.debugPoolState.textContent = "进程池状态读取失败，显示的调用记录仅代表此前响应。";
      if (!initialized) elements.monitorEmptyMessage.textContent = "暂时无法读取进程池状态，请确认服务已启动后重试。";
      if (!events[0] || events[0].message !== "无法读取进程池状态") {
        addEvent("无法读取进程池状态", "bad");
      }
      elements.updatedAt.textContent = `刷新失败 · ${error.message}`;
    } finally {
      refreshing = false;
    }
  }

  elements.pauseButton.addEventListener("click", () => {
    paused = !paused;
    elements.pauseButton.textContent = paused ? "继续刷新" : "暂停刷新";
    if (paused) {
      setConnection("paused", "刷新已暂停");
      addEvent("已暂停自动刷新", "info");
    } else {
      addEvent("已恢复自动刷新", "good");
      refresh();
    }
  });

  elements.refreshButton.addEventListener("click", () => {
    if (!factoriesReady) loadFactories();
    if (paused) {
      paused = false;
      elements.pauseButton.textContent = "暂停刷新";
    }
    refresh();
  });

  elements.initializationForm.addEventListener("submit", (event) => {
    event.preventDefault();
    if (changingPool || debugging || initialized || !elements.initializationForm.reportValidity()) return;
    const params = readInitializationParams();
    const { core_pool_size: core, maximum_pool_size: maximum, keep_alive_time: keepAlive } = params;
    const capacity = params.work_queue.capacity;
    if (![core, maximum, keepAlive, capacity].every(Number.isSafeInteger) || core > maximum) {
      showActionMessage("请使用有效整数，并确保核心进程数不大于最大进程数。", true);
      return;
    }
    changePool("pool.initialize", params);
  });

  elements.prestartButton.addEventListener("click", () => changePool("pool.prestart", {}));

  debuggerUi = ProcessPoolDebugger.mount({
    client: rpcClient,
    readInitializationParams,
    onBusyChange(value) {
      debugging = value;
      showLifecycle(initialized);
    },
    onSettled(records) {
      stateGeneration += 1;
      if (records.some((record) => record.request.method === "pool.initialize" && record.status === "success")) showLifecycle(true);
      paused = false;
      elements.pauseButton.textContent = "暂停刷新";
      refresh();
    },
  });

  new ResizeObserver(drawChart).observe(elements.chart);
  agentsUi = ProcessPoolAgents.mount({ client: ProcessPoolRpc.createClient() });
  elements.refreshButton.addEventListener("click", () => agentsUi.refresh());
  showTab(window.location.hash.slice(1), { syncHash: false });
  elements.connectionEndpoint.textContent = window.location.host;
  loadFactories();
  refresh();
  window.setInterval(refresh, 1000);
})();
