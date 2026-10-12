import { describe, expect, it } from "vitest";
import { runCmdAsync } from "./runtime";

describe("bounded asynchronous workflow execution", () => {
  it("keeps the event loop responsive and preserves Unicode from pipe chunks", async () => {
    let timerRan = false;
    let finished = false;
    const running = runCmdAsync(process.execPath, ["-e", "setTimeout(() => process.stdout.write(JSON.stringify({ok:true,text:'番組'})), 150)"]);
    running.then(() => { finished = true; });
    await new Promise<void>((resolve) => setTimeout(() => { timerRan = true; resolve(); }, 10));
    expect(timerRan).toBe(true);
    expect(finished).toBe(false);
    const result = await running;
    expect(result.ok).toBe(true);
    expect(JSON.parse(result.stdout).text).toBe("番組");
  });

  it("does not treat nonzero exit with JSON ok as process success", async () => {
    const result = await runCmdAsync(process.execPath, ["-e", "process.stdout.write('{\"ok\":true}');process.exit(7)"]);
    expect(result).toMatchObject({ ok: false, code: 7, stdout: '{"ok":true}' });
  });

  it("reports timeout and bounded output as failures", async () => {
    const timeout = await runCmdAsync(process.execPath, ["-e", "setInterval(()=>{},1000)"], undefined, {timeoutMs: 100});
    expect(timeout.ok).toBe(false);
    expect(timeout.stderr).toContain("deadline exceeded");
    const output = await runCmdAsync(process.execPath, ["-e", "process.stdout.write('x'.repeat(10000))"], undefined, {maxOutputBytes: 100});
    expect(output.ok).toBe(false);
    expect(output.stdout.length).toBeLessThanOrEqual(100);
    expect(output.stderr).toContain("output limit");
  });

  it("reports missing executable without throwing", async () => {
    const result = await runCmdAsync("/definitely/missing/video-workflow", []);
    expect(result.ok).toBe(false);
    expect(result.stderr).toContain("ENOENT");
  });
});
