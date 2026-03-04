import { parseJsonObject, resolvePythonScript, runCmd, toToolResult } from "./runtime";
import type { AnyObj } from "./types";

export function registerToolDbBackup(api: any, getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_db_backup",
      description: "DB バックアップ管理: backup (スナップショット作成) / list (一覧) / rotate (古いバックアップ削除)。",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          action: {
            type: "string",
            enum: ["backup", "list", "rotate"],
            default: "backup",
            description: "実行するアクション (backup | list | rotate)",
          },
          descriptor: {
            type: "string",
            description: "バックアップ名のサフィックス (action=backup 時のみ有効、例: pre_apply)",
          },
          keep: {
            type: "integer",
            minimum: 1,
            default: 10,
            description: "保持するバックアップ件数 (action=rotate 時のみ有効)",
          },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const resolved = resolvePythonScript("backup_mediaops_db.py");
        const action = typeof params.action === "string" ? params.action : "backup";

        const args = [
          "run", "python", resolved.scriptPath,
          "--db", String(cfg.db || ""),
          "--action", action,
        ];

        if (action === "backup" && typeof params.descriptor === "string" && params.descriptor.trim()) {
          args.push("--descriptor", params.descriptor.trim());
        }
        if (action === "rotate") {
          const keep = typeof params.keep === "number" && Number.isFinite(params.keep) ? Math.trunc(params.keep) : 10;
          args.push("--keep", String(keep));
        }

        const r = runCmd("uv", args, resolved.cwd);
        const parsed = parseJsonObject(r.stdout);
        const out: AnyObj = {
          ok: r.ok,
          tool: "video_pipeline_db_backup",
          action,
          exitCode: r.code,
          stdout: r.stdout,
          stderr: r.stderr,
        };
        if (parsed) {
          for (const [k, v] of Object.entries(parsed)) out[k] = v;
        }
        return toToolResult(out);
      },
    },
    { optional: true },
  );
}

export function registerToolDbRestore(api: any, getCfg: (api: any) => any) {
  api.registerTool(
    {
      name: "video_pipeline_db_restore",
      description: "DB をバックアップからリストアする。リストア前に現在の DB を自動退避。",
      parameters: {
        type: "object",
        additionalProperties: false,
        required: ["backupPath"],
        properties: {
          backupPath: {
            type: "string",
            description: "リストア元のバックアップファイルパス (必須)",
          },
        },
      },
      async execute(_id: string, params: AnyObj) {
        const cfg = getCfg(api);
        const resolved = resolvePythonScript("backup_mediaops_db.py");

        const backupPath = typeof params.backupPath === "string" ? params.backupPath.trim() : "";
        if (!backupPath) {
          return toToolResult({
            ok: false,
            tool: "video_pipeline_db_restore",
            error: "backupPath is required",
          });
        }

        const args = [
          "run", "python", resolved.scriptPath,
          "--db", String(cfg.db || ""),
          "--action", "restore",
          "--backup-path", backupPath,
        ];

        const r = runCmd("uv", args, resolved.cwd);
        const parsed = parseJsonObject(r.stdout);
        const out: AnyObj = {
          ok: r.ok,
          tool: "video_pipeline_db_restore",
          backupPath,
          exitCode: r.code,
          stdout: r.stdout,
          stderr: r.stderr,
        };
        if (parsed) {
          for (const [k, v] of Object.entries(parsed)) out[k] = v;
        }
        return toToolResult(out);
      },
    },
    { optional: true },
  );
}
