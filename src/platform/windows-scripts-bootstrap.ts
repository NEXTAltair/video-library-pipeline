import fs from "node:fs";
import path from "node:path";
import { getExtensionRootDir } from "./runtime";

export const REQUIRED_WINDOWS_SCRIPTS = [
  "unwatched_inventory.ps1",
  "apply_move_plan.ps1",
] as const;

export const INTERNAL_WINDOWS_SCRIPTS = [
  "_long_path_utils.ps1",
  "enumerate_files_jsonl.ps1",
  "normalize_case_dirs.ps1",
] as const;

export const MANAGED_WINDOWS_SCRIPTS = [...REQUIRED_WINDOWS_SCRIPTS, ...INTERNAL_WINDOWS_SCRIPTS] as const;

export type ProvisionFailure = {
  name: string;
  path: string;
  error: string;
};

export type ProvisionResult = {
  ok: boolean;
  scriptsDir: string;
  created: string[];
  updated: string[];
  existing: string[];
  failed: ProvisionFailure[];
  missingTemplates: string[];
};

export const WINDOWS_SCRIPT_TEMPLATE_RELATIVE_DIRS = [
  path.join("templates", "windows-scripts"),
  path.join("assets", "windows-scripts"),
] as const;

export function windowsScriptTemplateRoots(rootDir = getExtensionRootDir()): string[] {
  return WINDOWS_SCRIPT_TEMPLATE_RELATIVE_DIRS.map((relPath) => path.join(rootDir, relPath));
}

export function resolveWindowsScriptTemplatePath(
  name: string,
  rootDir = getExtensionRootDir(),
): string | null {
  for (const templateRoot of windowsScriptTemplateRoots(rootDir)) {
    const bundledTemplate = path.join(templateRoot, name);
    if (fs.existsSync(bundledTemplate)) return bundledTemplate;
  }
  return null;
}

export function ensureWindowsScripts(cfg: { windowsOpsRoot: string }): ProvisionResult {
  const scriptsDir = path.join(String(cfg.windowsOpsRoot || ""), "scripts");
  const result: ProvisionResult = {
    ok: false,
    scriptsDir,
    created: [],
    updated: [],
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

  for (const name of MANAGED_WINDOWS_SCRIPTS) {
    const target = path.join(scriptsDir, name);
    const template = resolveWindowsScriptTemplatePath(name);

    if (!template) {
      result.missingTemplates.push(name);
      continue;
    }

    if (fs.existsSync(target)) {
      try {
        const targetBuf = fs.readFileSync(target);
        const templateBuf = fs.readFileSync(template);
        if (Buffer.compare(targetBuf, templateBuf) === 0) {
          result.existing.push(target);
          continue;
        }
        fs.copyFileSync(template, target);
        result.updated.push(target);
        continue;
      } catch (e: any) {
        result.failed.push({
          name,
          path: target,
          error: String(e?.message || e),
        });
        continue;
      }
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
