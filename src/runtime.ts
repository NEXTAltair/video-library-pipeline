import crypto from "node:crypto";
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

// JSON文字列からオブジェクトをパースし、配列やプリミティブは除外して返す。
export function parseJsonObject(input: unknown): Record<string, any> | null {
  if (typeof input !== "string") return null;
  const s = input.trim();
  if (!s) return null;
  try {
    const parsed = JSON.parse(s);
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed))
      return parsed as Record<string, any>;
    return null;
  } catch {
    return null;
  }
}

// コンパクトなタイムスタンプ文字列を生成 (例: 20260304_153042)。
export function tsCompact(d = new Date()): string {
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}_${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}

// ミリ秒精度のタイムスタンプ (例: 20260304_153042_789)。同一秒内の衝突を防止。
export function tsCompactMs(d = new Date()): string {
  return `${tsCompact(d)}_${String(d.getMilliseconds()).padStart(3, "0")}`;
}

// コンテンツの短縮 SHA256 ハッシュ (先頭 length hex 文字)。
export function sha256Short(content: string, length = 16): string {
  return crypto.createHash("sha256").update(content, "utf-8").digest("hex").slice(0, length);
}

// LLM抽出出力のJSONLファイルを選択する。明示パス優先、なければ最新を自動選択。
export function chooseSourceJsonl(
  llmDir: string,
  sourceJsonlPath: string | undefined,
): { ok: boolean; path?: string; error?: string } {
  const p = String(sourceJsonlPath || "").trim();
  if (p) {
    if (!fs.existsSync(p))
      return { ok: false, error: `sourceJsonlPath does not exist: ${p}` };
    return { ok: true, path: p };
  }
  const latest = latestJsonlFile(llmDir, "llm_filename_extract_output_");
  if (!latest)
    return {
      ok: false,
      error: `no extraction output jsonl found in: ${llmDir}`,
    };
  return { ok: true, path: latest };
}

// タイトルをファイルシステム安全な正規化キーに変換。
export function normalizeKey(title: string): string {
  return String(title || "")
    .trim()
    .replace(/\s+/g, "_")
    .replace(/[<>:"/\\|?*]+/g, "")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "")
    .toLowerCase();
}

// 文字列を正規化・小文字化し空白と特殊文字を除去。比較用。
export function lowerCompact(s: string): string {
  return String(s || "")
    .normalize("NFKC")
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/[<>:"/\\|?*]+/g, "");
}

// Windowsパスから by_program 直下のフォルダ名（番組グループ）を抽出。
export function byProgramGroupFromPath(
  winPath: string | undefined,
): string | undefined {
  const parts = String(winPath || "")
    .split(/[\\/]+/)
    .filter(Boolean);
  const idx = parts.findIndex((p) => p.toLowerCase() === "by_program");
  if (idx >= 0 && idx + 1 < parts.length) return parts[idx + 1];
  return undefined;
}

// program_titleがフォルダ名を飲み込んでいる（フォルダ名+8文字以上のサフィックス）かを判定。
export function looksSwallowedProgramTitle(
  programTitle: string,
  folderTitle: string,
): boolean {
  const p = programTitle.trim();
  const f = folderTitle.trim();
  if (!p || !f) return false;
  if (p === f) return false;
  const pNorm = lowerCompact(p);
  const fNorm = lowerCompact(f);
  if (!pNorm || !fNorm) return false;
  if (!pNorm.startsWith(fNorm)) return false;
  return pNorm.length >= fNorm.length + 8;
}
