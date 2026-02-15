import fs from "node:fs";
import path from "node:path";
import { getExtensionRootDir, runCmd } from "./runtime";
import type { AnyObj } from "./types";

const TARGET_TOOL = "video_pipeline_analyze_and_move_videos";
const ALERT_MARKER = "[hook-alert]";

const REQUIRED_WINDOWS_SCRIPTS = [
  "normalize_filenames.ps1",
  "unwatched_inventory.ps1",
  "apply_move_plan.ps1",
  "list_remaining_unwatched.ps1",
];

function isObj(v: unknown): v is AnyObj {
  return !!v && typeof v === "object" && !Array.isArray(v);
}

function parseJsonObject(input: string): AnyObj | null {
  const s = String(input || "").trim();
  if (!s) return null;
  try {
    const parsed = JSON.parse(s);
    return isObj(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function parseSummaryFromStdout(stdout: unknown): AnyObj | null {
  if (typeof stdout !== "string") return null;
  return parseJsonObject(stdout);
}

function getToolEnvelopeFromResult(result: unknown): AnyObj | null {
  if (!isObj(result)) return null;
  const content = Array.isArray(result.content) ? result.content : [];
  for (const item of content) {
    if (!isObj(item) || item.type !== "text" || typeof item.text !== "string") continue;
    const parsed = parseJsonObject(item.text);
    if (parsed) return parsed;
  }
  return null;
}

function collectApplyPreflightIssues(cfg: AnyObj): string[] {
  const issues: string[] = [];
  const dbDir = typeof cfg.db === "string" ? path.dirname(cfg.db) : "";
  const opsRoot = typeof cfg.windowsOpsRoot === "string" ? cfg.windowsOpsRoot : "";
  const scriptsDir = opsRoot ? path.join(opsRoot, "scripts") : "";
  const hintsPath = path.join(getExtensionRootDir(), "rules", "program_aliases.yaml");

  if (!dbDir || !fs.existsSync(dbDir)) issues.push(`db parent dir missing: ${dbDir || "(empty)"}`);
  if (!opsRoot || !fs.existsSync(opsRoot)) issues.push(`windowsOpsRoot missing: ${opsRoot || "(empty)"}`);
  if (!scriptsDir || !fs.existsSync(scriptsDir)) issues.push(`scripts dir missing: ${scriptsDir || "(empty)"}`);
  if (!fs.existsSync(hintsPath)) issues.push(`hints yaml missing: ${hintsPath}`);

  for (const name of REQUIRED_WINDOWS_SCRIPTS) {
    const p = path.join(scriptsDir, name);
    if (!fs.existsSync(p)) issues.push(`required script missing: ${p}`);
  }

  const uv = runCmd("uv", ["--version"]);
  if (!uv.ok) issues.push("uv is not available");
  const py = runCmd("uv", ["run", "python", "--version"]);
  if (!py.ok) issues.push("python via uv is not available");

  let pw = runCmd("pwsh", ["-NoProfile", "-Command", "$PSVersionTable.PSVersion.ToString()"]);
  if (!pw.ok) pw = runCmd("pwsh.exe", ["-NoProfile", "-Command", "$PSVersionTable.PSVersion.ToString()"]);
  if (!pw.ok) issues.push("pwsh/pwsh.exe is not available");

  return issues;
}

function analyzeToolEnvelope(toolEnvelope: AnyObj | null, eventError?: unknown): string[] {
  const alerts: string[] = [];
  if (typeof eventError === "string" && eventError.trim()) alerts.push(`tool-error: ${eventError.trim()}`);

  if (!toolEnvelope) {
    alerts.push("tool-output-missing");
    return alerts;
  }

  const exitCode = Number(toolEnvelope.exitCode ?? 0);
  if (Number.isFinite(exitCode) && exitCode !== 0) alerts.push(`exit-code: ${exitCode}`);

  const summary = parseSummaryFromStdout(toolEnvelope.stdout);
  if (!summary) {
    alerts.push("summary-parse-failed");
    return alerts;
  }

  const remaining = Number(summary.remaining_files ?? 0);
  const apply = summary.apply === true;
  if (apply && Number.isFinite(remaining) && remaining > 0) alerts.push(`remaining-files: ${remaining}`);

  const planStats = isObj(summary.plan_stats) ? summary.plan_stats : null;
  if (planStats) {
    const needsReview = Number(planStats.skipped_needs_review ?? 0);
    const missingFields = Number(planStats.skipped_missing_fields ?? 0);
    const outside = Number(planStats.skipped_outside ?? 0);
    if (Number.isFinite(needsReview) && needsReview > 0) alerts.push(`skipped-needs-review: ${needsReview}`);
    if (Number.isFinite(missingFields) && missingFields > 0) alerts.push(`skipped-missing-fields: ${missingFields}`);
    if (Number.isFinite(outside) && outside > 0) alerts.push(`skipped-outside: ${outside}`);
  }

  const requiredPointers = ["inventory", "queue", "plan", "applied"] as const;
  for (const key of requiredPointers) {
    const v = summary[key];
    if (typeof v !== "string" || !v.trim()) alerts.push(`missing-pointer: ${key}`);
  }

  return Array.from(new Set(alerts));
}

export function registerPluginHooks(api: any, getCfg: (api: any) => AnyObj) {
  api.on("before_tool_call", (event: AnyObj) => {
    if (event?.toolName !== TARGET_TOOL) return;
    const params = isObj(event?.params) ? event.params : {};

    // 安全上、レビュー許容での実applyはブロックする。
    if (params.apply === true && params.allowNeedsReview === true) {
      return {
        block: true,
        blockReason: `${TARGET_TOOL} blocked: apply=true with allowNeedsReview=true is not allowed.`,
      };
    }

    if (params.apply !== true) return;

    let cfg: AnyObj;
    try {
      cfg = getCfg(api);
    } catch (e: any) {
      return {
        block: true,
        blockReason: `${TARGET_TOOL} preflight failed: ${String(e?.message || e)}`,
      };
    }

    const issues = collectApplyPreflightIssues(cfg);
    if (issues.length === 0) return;
    return {
      block: true,
      blockReason: `${TARGET_TOOL} preflight failed:\n- ${issues.join("\n- ")}`,
    };
  });

  api.on("after_tool_call", (event: AnyObj, ctx: AnyObj) => {
    if (event?.toolName !== TARGET_TOOL) return;
    const toolEnvelope = getToolEnvelopeFromResult(event.result);
    const alerts = analyzeToolEnvelope(toolEnvelope, event.error);
    if (alerts.length === 0) return;
    api.logger?.warn?.(
      `[${TARGET_TOOL}] alerts session=${String(ctx?.sessionKey || "")} tool=${String(event?.toolName || "")} ${alerts.join("; ")}`,
    );
  });

  // tool_result_persist は同期必須。
  api.on(
    "tool_result_persist",
    (event: AnyObj) => {
      if (event?.toolName !== TARGET_TOOL) return;
      const message = isObj(event?.message) ? event.message : null;
      if (!message || !Array.isArray(message.content)) return;

      const toolEnvelope = getToolEnvelopeFromResult(message);
      const alerts = analyzeToolEnvelope(toolEnvelope);
      if (alerts.length === 0) return;

      const exists = message.content.some(
        (item: unknown) =>
          isObj(item) &&
          item.type === "text" &&
          typeof item.text === "string" &&
          item.text.includes(ALERT_MARKER),
      );
      if (exists) return;

      return {
        message: {
          ...message,
          content: [
            ...message.content,
            { type: "text", text: `${ALERT_MARKER} ${alerts.join(" | ")}` },
          ],
        },
      };
    },
    { priority: 50 },
  );
}
