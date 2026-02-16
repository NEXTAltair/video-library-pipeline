import fs from "node:fs";
import path from "node:path";
import { getExtensionRootDir } from "./runtime";

export const REQUIRED_WINDOWS_SCRIPTS = [
  "normalize_filenames.ps1",
  "unwatched_inventory.ps1",
  "apply_move_plan.ps1",
  "list_remaining_unwatched.ps1",
] as const;

export type ProvisionFailure = {
  name: string;
  path: string;
  error: string;
};

export type ProvisionResult = {
  ok: boolean;
  scriptsDir: string;
  created: string[];
  existing: string[];
  failed: ProvisionFailure[];
  missingTemplates: string[];
};

function templateRootDir(): string {
  return path.join(getExtensionRootDir(), "assets", "windows-scripts");
}

function userTemplateRootDir(cfg: { windowsOpsRoot: string }): string {
  return path.join(String(cfg.windowsOpsRoot || ""), "templates", "windows-scripts");
}

function resolveTemplatePath(cfg: { windowsOpsRoot: string }, name: string): string | null {
  const userTemplate = path.join(userTemplateRootDir(cfg), name);
  if (fs.existsSync(userTemplate)) return userTemplate;

  const bundledTemplate = path.join(templateRootDir(), name);
  if (fs.existsSync(bundledTemplate)) return bundledTemplate;

  return null;
}

export function ensureWindowsScripts(cfg: { windowsOpsRoot: string }): ProvisionResult {
  const scriptsDir = path.join(String(cfg.windowsOpsRoot || ""), "scripts");
  const result: ProvisionResult = {
    ok: false,
    scriptsDir,
    created: [],
    existing: [],
    failed: [],
    missingTemplates: [],
  };

  try {
    fs.mkdirSync(scriptsDir, { recursive: true });
  } catch (e: any) {
    result.failed.push({
      name: "__scripts_dir__",
      path: scriptsDir,
      error: String(e?.message || e),
    });
    result.ok = false;
    return result;
  }

  for (const name of REQUIRED_WINDOWS_SCRIPTS) {
    const target = path.join(scriptsDir, name);
    const template = resolveTemplatePath(cfg, name);

    if (!template) {
      result.missingTemplates.push(name);
      continue;
    }

    if (fs.existsSync(target)) {
      result.existing.push(target);
      continue;
    }

    try {
      fs.copyFileSync(template, target);
      result.created.push(target);
    } catch (e: any) {
      result.failed.push({
        name,
        path: target,
        error: String(e?.message || e),
      });
    }
  }

  result.ok = result.failed.length === 0 && result.missingTemplates.length === 0;
  return result;
}
