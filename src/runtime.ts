import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import type { CmdResult } from "./types";

// 共通コマンド実行ヘルパー。
// 例外を投げず、成功可否と stdout/stderr を構造化して返す。
export function runCmd(command: string, args: string[], cwd?: string): CmdResult {
  const cp = spawnSync(command, args, {
    cwd,
    env: process.env,
    encoding: "utf-8",
  });
  return {
    ok: cp.status === 0,
    code: cp.status ?? 1,
    stdout: cp.stdout ?? "",
    stderr: cp.stderr ?? "",
    command,
    args,
    cwd,
  };
}

const EXT_SRC_DIR = path.dirname(fileURLToPath(import.meta.url));
const EXT_ROOT_DIR = path.resolve(EXT_SRC_DIR, "..");
const EXT_PY_DIR = path.join(EXT_ROOT_DIR, "py");

export function getExtensionRootDir(): string {
  return EXT_ROOT_DIR;
}

export function getExtensionPyDir(): string {
  return EXT_PY_DIR;
}

// extension配下のpyを常に正とする。
export function resolvePythonScript(scriptName: string): {
  scriptPath: string;
  cwd: string;
  source: "extension";
} {
  const extPath = path.join(EXT_PY_DIR, scriptName);
  return { scriptPath: extPath, cwd: EXT_PY_DIR, source: "extension" };
}

// 指定ディレクトリから prefix に一致する最新 JSONL を1件返す。
export function latestJsonlFile(dir: string, prefix: string): string | null {
  if (!fs.existsSync(dir)) return null;
  const files = fs
    .readdirSync(dir)
    .filter((n) => n.startsWith(prefix) && n.endsWith(".jsonl"))
    .map((n) => path.join(dir, n))
    .map((p) => ({ p, m: fs.statSync(p).mtimeMs }))
    .sort((a, b) => b.m - a.m);
  return files[0]?.p ?? null;
}

// OpenClaw tool の戻り値形式へ整形。
export function toToolResult(obj: Record<string, unknown>) {
  return { content: [{ type: "text", text: JSON.stringify(obj, null, 2) }] };
}
