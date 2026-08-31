#!/usr/bin/env node
// Offline protocol fixture; never contacts Claude or reads credentials.
const readline = require("node:readline");
const assert = require("node:assert/strict");
if (process.argv.includes("--version")) { console.log("fake Claude Code (test)"); process.exit(0); }
for (const flag of ["--safe-mode", "--permission-prompt-tool", "--input-format", "--output-format"]) assert.ok(process.argv.includes(flag));
assert.ok(!process.argv.includes("--dangerously-skip-permissions"));
const resume = process.argv.indexOf("--resume");
const session = resume < 0 ? `fake-session-${process.pid}` : process.argv[resume + 1];
let turn = 0;
const emit = (message) => process.stdout.write(`${JSON.stringify(message)}\n`);
const result = (text, extra = {}) => emit({ type: "result", subtype: "success", is_error: false, result: text, session_id: session, ...extra });
readline.createInterface({ input: process.stdin }).on("line", (line) => {
  const message = JSON.parse(line);
  if (message.type === "control_request") {
    emit({ type: "control_response", response: { subtype: "success", request_id: message.request_id, response: {} } });
    if (message.request.subtype === "interrupt") result("interrupted");
  } else if (message.type === "user") {
    const text = message.message.content;
    turn += 1;
    emit({ type: "system", subtype: "init", session_id: session });
    if (text === "crash") { process.exit(7); }
    if (text === "hang") { return; }
    if (text === "child") {
      const child = require("node:child_process").spawn(process.execPath, ["-e", "setTimeout(() => {}, 45000)"], { stdio: "ignore" });
      result(JSON.stringify({ child_pid: child.pid }));
      return;
    }
    if (text === "oversized") { process.stdout.write("x".repeat(1024 * 1024 + 1)); return; }
    if (text === "permission") {
      emit({ type: "control_request", request_id: "permission-1", request: { subtype: "can_use_tool", tool_name: "Bash", input: { command: "echo fixture-only" } } });
      return;
    }
    if (text === "flood") for (let i = 0; i < 300; i++) emit({ type: "system", subtype: `event-${i}` });
    if (text === "huge-event") emit({ type: "assistant", message: { content: [{ type: "text", text: "中".repeat(10000) }] } });
    emit({ type: "assistant", session_id: session, message: { content: [{ type: "text", text: `${text} turn=${turn}` }] } });
    result(JSON.stringify({ pid: process.pid, cwd: process.cwd(), turn, text }));
  } else if (message.type === "control_response") {
    const answer = message.response.response;
    if (answer.behavior === "allow") assert.deepEqual(answer.updatedInput, { command: "echo fixture-only" });
    result(`permission:${answer.behavior}`);
  }
});
