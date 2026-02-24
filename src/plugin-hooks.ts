import fs from "node:fs";
import path from "node:path";
import { getExtensionRootDir, runCmd } from "./runtime";
import type { AnyObj } from "./types";
import { REQUIRED_WINDOWS_SCRIPTS, ensureWindowsScripts } from "./windows-scripts-bootstrap";

const TARGET_TOOL = "video_pipeline_analyze_and_move_videos";
const RELOCATE_TOOL = "video_pipeline_relocate_existing_files";
const APPLY_PREFLIGHT_TOOLS = new Set([TARGET_TOOL, RELOCATE_TOOL]);
const ALERT_MARKER = "[hook-alert]";
const TOOL_NAME_REGEX = /^video_pipeline_[a-z0-9_]+$/;
const CALIBRE_SERVER_TOKENS = new Set(["calibre-server", "calibre-server.exe"]);
const CALIBRE_DB_TOKENS = new Set(["calibredb", "calibredb.exe"]);
const CALIBRE_AUTH_MODE_ARG_RE = /(^|\s)--(?:auth-mode|auth_mode|auth-scheme|auth_scheme)(?:\s|=|$)/i;
const CALIBRE_TOOL_CMD_RE =
  /\b(?:calibredb(?:\.exe)?|calibredb_read\.mjs|calibredb_apply\.mjs|run_analysis_pipeline\.py)\b/i;
const BAD_LOCAL_CALIBRE_LIBRARY_PATTERNS = [
  /(^|\s)(["']?)~\/calibre library\2(\s|$)/i,
  /(^|\s)(["']?)\/home\/altair\/calibre library\2(\s|$)/i,
];
const CALIBRE_READ_ALLOWED_SUBCOMMANDS = new Set(["list", "search", "id"]);
const CALIBRE_GATE_TTL_MS = 10 * 60 * 1000;
const CALIBRE_METADATA_SKILL_PATH_RE = /(^|\/)skills\/calibre-metadata-apply\/SKILL\.md$/i;
const CALIBRE_HINT_INTENT_RE = /(calibre|カリバー|calibredb|content server|--with-library)/i;
const CALIBRE_EDIT_INTENT_RE =
  /(メタデータ|metadata|タイトル|title|authors?|author|series(?:_index)?|tags?|publisher|pubdate|languages?|修正|編集|更新|fix|update|edit)/i;
const CALIBRE_BOOK_ID_RE = /\b(?:id[:#\s-]*\d{2,7}|ID\d{2,7})\b/i;

type CalibreSkillGateState = {
  activatedAt: number;
  skillDocRead: boolean;
};

const calibreSkillGateBySession = new Map<string, CalibreSkillGateState>();

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

function collectApplyPreflightIssues(cfg: AnyObj, opts?: { requireHintsYaml?: boolean }): string[] {
  const issues: string[] = [];
  const dbDir = typeof cfg.db === "string" ? path.dirname(cfg.db) : "";
  const opsRoot = typeof cfg.windowsOpsRoot === "string" ? cfg.windowsOpsRoot : "";
  const scriptsDir = opsRoot ? path.join(opsRoot, "scripts") : "";
  const hintsPath = path.join(getExtensionRootDir(), "rules", "program_aliases.yaml");
  const requireHintsYaml = opts?.requireHintsYaml !== false;

  if (!dbDir || !fs.existsSync(dbDir)) issues.push(`db parent dir missing: ${dbDir || "(empty)"}`);
  if (!opsRoot || !fs.existsSync(opsRoot)) issues.push(`windowsOpsRoot missing: ${opsRoot || "(empty)"}`);
  if (!scriptsDir || !fs.existsSync(scriptsDir)) issues.push(`scripts dir missing: ${scriptsDir || "(empty)"}`);
  if (requireHintsYaml && !fs.existsSync(hintsPath)) issues.push(`hints yaml missing: ${hintsPath}`);

  for (const name of REQUIRED_WINDOWS_SCRIPTS) {
    const p = path.join(scriptsDir, name);
    if (!fs.existsSync(p)) issues.push(`required script missing: ${p}`);
  }

  const uv = runCmd("uv", ["--version"]);
  if (!uv.ok) issues.push("uv is not available");
  const py = runCmd("uv", ["run", "python", "--version"]);
  if (!py.ok) issues.push("python via uv is not available");

  const pwshCandidates = ["/mnt/c/Program Files/PowerShell/7/pwsh.exe", "pwsh", "pwsh.exe"];
  let pw = runCmd(pwshCandidates[0], ["-NoProfile", "-Command", "$PSVersionTable.PSVersion.ToString()"]);
  for (let i = 1; !pw.ok && i < pwshCandidates.length; i += 1) {
    pw = runCmd(pwshCandidates[i], ["-NoProfile", "-Command", "$PSVersionTable.PSVersion.ToString()"]);
  }
  if (!pw.ok) issues.push("pwsh7 is not available");

  const regCandidates = ["/mnt/c/Windows/System32/reg.exe", "reg.exe"];
  let reg = runCmd(regCandidates[0], [
    "query",
    "HKLM\\SYSTEM\\CurrentControlSet\\Control\\FileSystem",
    "/v",
    "LongPathsEnabled",
  ]);
  if (!reg.ok && regCandidates.length > 1) {
    reg = runCmd(regCandidates[1], ["query", "HKLM\\SYSTEM\\CurrentControlSet\\Control\\FileSystem", "/v", "LongPathsEnabled"]);
  }
  if (!reg.ok) {
    issues.push("failed to query Windows LongPathsEnabled registry value");
  } else {
    const m = reg.stdout.match(/LongPathsEnabled\s+REG_DWORD\s+0x([0-9a-fA-F]+)/);
    const enabled = !!m && Number.parseInt(m[1], 16) === 1;
    if (!enabled) issues.push("Windows LongPathsEnabled is not enabled (expected REG_DWORD 0x1)");
  }

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

function detectMistakenExecToolName(event: AnyObj): string | null {
  if (event?.toolName !== "exec") return null;
  const command = getExecCommand(event);
  if (!command) return null;
  const firstToken = command.split(/\s+/, 1)[0] || "";
  if (!TOOL_NAME_REGEX.test(firstToken)) return null;
  return firstToken;
}

function getExecCommand(event: AnyObj): string {
  if (event?.toolName !== "exec") return "";
  const params = isObj(event?.params) ? event.params : {};
  const byCommand = typeof params.command === "string" ? params.command : "";
  const byCmd = typeof params.cmd === "string" ? params.cmd : "";
  return String(byCommand || byCmd || "").trim();
}

function detectBlockedCalibreServerExec(event: AnyObj): string | null {
  const command = getExecCommand(event);
  if (!command) return null;
  const firstToken = command.split(/\s+/, 1)[0]?.toLowerCase() || "";
  if (!CALIBRE_SERVER_TOKENS.has(firstToken)) return null;
  return command;
}

function detectBadLocalCalibreLibraryPath(event: AnyObj): string | null {
  const command = getExecCommand(event);
  if (!command) return null;
  const firstToken = command.split(/\s+/, 1)[0]?.toLowerCase() || "";
  if (firstToken !== "calibredb" && firstToken !== "calibredb.exe") return null;
  for (const re of BAD_LOCAL_CALIBRE_LIBRARY_PATTERNS) {
    if (re.test(command)) return command;
  }
  return null;
}

function detectMisusedCalibreReadForEdit(event: AnyObj): { subcommand: string; command: string } | null {
  const command = getExecCommand(event);
  if (!command) return null;
  const m = command.match(/\bcalibredb_read\.mjs\b(?:\s+([A-Za-z0-9_-]+))?/i);
  if (!m) return null;
  const sub = String(m[1] || "").trim().toLowerCase();
  if (!sub) return null;
  if (CALIBRE_READ_ALLOWED_SUBCOMMANDS.has(sub)) return null;
  return { subcommand: sub, command };
}

function detectDirectCalibreDbWithoutSkill(event: AnyObj): string | null {
  const command = getExecCommand(event);
  if (!command) return null;
  const firstToken = command.split(/\s+/, 1)[0]?.toLowerCase() || "";
  if (!CALIBRE_DB_TOKENS.has(firstToken)) return null;
  return command;
}

function detectCalibreAuthModeArg(event: AnyObj): string | null {
  const command = getExecCommand(event);
  if (!command) return null;
  if (!CALIBRE_TOOL_CMD_RE.test(command)) return null;
  if (!CALIBRE_AUTH_MODE_ARG_RE.test(command)) return null;
  return command;
}

function extractTextFromContent(content: unknown): string {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  const parts: string[] = [];
  for (const item of content) {
    if (typeof item === "string") {
      parts.push(item);
      continue;
    }
    if (!isObj(item)) continue;
    if (typeof item.text === "string") parts.push(item.text);
  }
  return parts.join("\n").trim();
}

function getLatestUserTextFromMessages(messages: unknown[]): string {
  for (let i = messages.length - 1; i >= 0; i -= 1) {
    const msg = messages[i];
    if (!isObj(msg) || String(msg.role || "") !== "user") continue;
    return extractTextFromContent((msg as AnyObj).content);
  }
  return "";
}

function isCalibreMetadataEditIntent(input: string): boolean {
  const text = String(input || "").replace(/\s+/g, " ").trim();
  if (!text) return false;
  const hasCalibreHint = CALIBRE_HINT_INTENT_RE.test(text);
  const hasEditIntent = CALIBRE_EDIT_INTENT_RE.test(text);
  const hasBookId = CALIBRE_BOOK_ID_RE.test(text);
  return (hasCalibreHint && hasEditIntent) || (hasBookId && hasEditIntent);
}

function cleanupCalibreGates() {
  const now = Date.now();
  for (const [sessionKey, state] of calibreSkillGateBySession.entries()) {
    if (now - state.activatedAt > CALIBRE_GATE_TTL_MS) {
      calibreSkillGateBySession.delete(sessionKey);
    }
  }
}

function rememberCalibreGate(sessionKey: string) {
  if (!sessionKey) return;
  cleanupCalibreGates();
  calibreSkillGateBySession.set(sessionKey, {
    activatedAt: Date.now(),
    skillDocRead: false,
  });
}

function getCalibreGate(sessionKey: string): CalibreSkillGateState | null {
  if (!sessionKey) return null;
  cleanupCalibreGates();
  return calibreSkillGateBySession.get(sessionKey) || null;
}

function markCalibreSkillRead(sessionKey: string) {
  const state = getCalibreGate(sessionKey);
  if (!state) return;
  calibreSkillGateBySession.set(sessionKey, {
    activatedAt: Date.now(),
    skillDocRead: true,
  });
}

function clearCalibreGate(sessionKey: string) {
  if (!sessionKey) return;
  calibreSkillGateBySession.delete(sessionKey);
}

function isReadOfCalibreMetadataSkill(event: AnyObj): boolean {
  if (event?.toolName !== "read") return false;
  const params = isObj(event?.params) ? event.params : {};
  const p =
    (typeof params.path === "string" && params.path) ||
    (typeof params.file_path === "string" && params.file_path) ||
    (typeof params.filePath === "string" && params.filePath) ||
    "";
  if (!p) return false;
  const normalized = p.replace(/\\/g, "/");
  return CALIBRE_METADATA_SKILL_PATH_RE.test(normalized);
}

function buildCalibrePromptNudge(): string {
  return [
    "Calibre metadata edit routing for this run:",
    "- This request is a metadata edit/fix intent. Use calibre-metadata-apply now.",
    "- First call `read` on `skills/calibre-metadata-apply/SKILL.md`.",
    "- Do not use memory_search/memory_get before reading that SKILL for this task.",
    "- Never start `calibre-server` from chat; connect to the running Content server.",
    "- Try saved defaults first (env/config) before asking for `--with-library` URL.",
    "- For ID-based title fix, fetch current metadata first and produce dry-run proposal before apply.",
  ].join("\n");
}

export function registerPluginHooks(api: any, getCfg: (api: any) => AnyObj) {
  api.on("before_prompt_build", (event: AnyObj, ctx: AnyObj) => {
    const prompt = typeof event?.prompt === "string" ? event.prompt : "";
    const messages = Array.isArray(event?.messages) ? event.messages : [];
    const latestUserText = getLatestUserTextFromMessages(messages);
    const combined = `${prompt}\n${latestUserText}`.trim();
    const sessionKey = typeof ctx?.sessionKey === "string" ? ctx.sessionKey : "";

    if (!isCalibreMetadataEditIntent(combined)) {
      clearCalibreGate(sessionKey);
      return;
    }

    rememberCalibreGate(sessionKey);
    return { prependContext: buildCalibrePromptNudge() };
  });

  api.on("before_tool_call", (event: AnyObj, ctx: AnyObj) => {
    const sessionKey = typeof ctx?.sessionKey === "string" ? ctx.sessionKey : "";
    const calibreGate = getCalibreGate(sessionKey);
    if (calibreGate && isReadOfCalibreMetadataSkill(event)) {
      markCalibreSkillRead(sessionKey);
    } else if (
      calibreGate &&
      !calibreGate.skillDocRead &&
      (event?.toolName === "memory_search" || event?.toolName === "memory_get")
    ) {
      return {
        block: true,
        blockReason:
          "blocked tool flow: Calibre metadata edit intent detected. " +
          "Read skills/calibre-metadata-apply/SKILL.md first, then continue with metadata-apply steps.",
      };
    }

    if (calibreGate && event?.toolName === "exec") {
      const command = getExecCommand(event);
      if (/\bcalibredb_apply\.mjs\b/i.test(command)) {
        clearCalibreGate(sessionKey);
      }
    }

    const blockedCalibreServer = detectBlockedCalibreServerExec(event);
    if (blockedCalibreServer) {
      return {
        block: true,
        blockReason:
          "blocked exec call: do not start calibre-server from agent. " +
          "Use the running Content server via --with-library http://HOST:PORT/#LIBRARY_ID " +
          "(or CALIBRE_WITH_LIBRARY/CALIBRE_CONTENT_SERVER_URL).",
      };
    }

    const authModeInCalibreCommand = detectCalibreAuthModeArg(event);
    if (authModeInCalibreCommand) {
      return {
        block: true,
        blockReason:
          "blocked exec call: auth mode arguments are not supported in this Calibre workflow. " +
          "Use non-SSL Digest auth policy with username/password only (no --auth-mode/--auth-scheme).",
      };
    }

    const directCalibreDb = detectDirectCalibreDbWithoutSkill(event);
    if (directCalibreDb) {
      return {
        block: true,
        blockReason:
          "blocked exec call: direct calibredb invocation is not allowed in chat flow. " +
          "Use skill scripts only: " +
          "node skills/calibre-catalog-read/scripts/calibredb_read.mjs (read) or " +
          "node skills/calibre-metadata-apply/scripts/calibredb_apply.mjs (edit).",
      };
    }

    const badLocalLibraryPath = detectBadLocalCalibreLibraryPath(event);
    if (badLocalLibraryPath) {
      return {
        block: true,
        blockReason:
          "blocked exec call: '~/Calibre Library' local-path guess is invalid for this environment. " +
          "Use remote Content server URL with --with-library http://HOST:PORT/#LIBRARY_ID.",
      };
    }

    const misusedCalibreRead = detectMisusedCalibreReadForEdit(event);
    if (misusedCalibreRead) {
      return {
        block: true,
        blockReason:
          `blocked exec call: calibredb_read.mjs does not support '${misusedCalibreRead.subcommand}'. ` +
          "Use list/search/id for reads, and use " +
          "node skills/calibre-metadata-apply/scripts/calibredb_apply.mjs for metadata edits.",
      };
    }

    const mistakenToolName = detectMistakenExecToolName(event);
    if (mistakenToolName) {
      return {
        block: true,
        blockReason:
          `blocked mistaken exec call: '${mistakenToolName}' is a plugin tool name, not a shell command. ` +
          `Call the tool directly as ${mistakenToolName} with JSON params.`,
      };
    }

    const hookToolName = String(event?.toolName || "");
    if (!APPLY_PREFLIGHT_TOOLS.has(hookToolName)) return;
    const params = isObj(event?.params) ? event.params : {};

    // 安全上、レビュー許容での実applyはブロックする。
    if (params.apply === true && params.allowNeedsReview === true) {
      return {
        block: true,
        blockReason: `${hookToolName} blocked: apply=true with allowNeedsReview=true is not allowed.`,
      };
    }

    if (params.apply !== true) return;

    let cfg: AnyObj;
    try {
      cfg = getCfg(api);
    } catch (e: any) {
      return {
        block: true,
        blockReason: `${hookToolName} preflight failed: ${String(e?.message || e)}`,
      };
    }

    const scriptsProvision = ensureWindowsScripts(cfg);
    if (!scriptsProvision.ok) {
      const issues: string[] = [];
      if (scriptsProvision.missingTemplates.length > 0) {
        issues.push(`missing script templates: ${scriptsProvision.missingTemplates.join(", ")}`);
      }
      for (const f of scriptsProvision.failed) {
        issues.push(`script provision failed (${f.name}): ${f.path} :: ${f.error}`);
      }
      return {
        block: true,
        blockReason: `${hookToolName} preflight failed:\n- ${issues.join("\n- ")}`,
      };
    }

    const issues = collectApplyPreflightIssues(cfg, { requireHintsYaml: hookToolName === TARGET_TOOL });
    if (issues.length === 0) return;
    return {
      block: true,
      blockReason: `${hookToolName} preflight failed:\n- ${issues.join("\n- ")}`,
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
