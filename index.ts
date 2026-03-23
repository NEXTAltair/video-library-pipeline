import { registerCli } from "./src/cli";
import { pluginId, getCfg } from "./src/plugin-meta";
import { registerPluginHooks } from "./src/plugin-hooks";
import { registerToolApplyReviewedMetadata } from "./src/tool-apply-reviewed-metadata";
import { registerToolDbBackup, registerToolDbRestore } from "./src/tool-db-backup";
import { registerToolBackfill } from "./src/tool-backfill";
import { registerToolDedup } from "./src/tool-dedup";
import { registerToolLogs } from "./src/tool-logs";
import { registerToolExportProgramYaml } from "./src/tool-export-program-yaml";
import { registerToolDetectFolderContamination } from "./src/tool-detect-folder-contamination";
import { registerToolDetectRebroadcasts } from "./src/tool-detect-rebroadcasts";
import { registerToolIngestEpg } from "./src/tool-ingest-epg";
import { registerToolPrepareRelocateMetadata } from "./src/tool-prepare-relocate-metadata";
import { registerToolRelocate } from "./src/tool-relocate";
import { registerToolNormalizeFolderCase } from "./src/tool-normalize-folder-case";
import { registerToolReextract } from "./src/tool-reextract";
import { registerToolLlmExtract } from "./src/tool-llm-extract";
import { registerToolLlmExtractStatus } from "./src/tool-llm-extract-status";
import { registerToolRepairDb } from "./src/tool-repair-db";
import { registerToolUpdateProgramTitles } from "./src/tool-update-program-titles";
import { registerToolRun } from "./src/tool-run";
import { registerToolStatus } from "./src/tool-status";
import { registerToolValidate } from "./src/tool-validate";

export default function register(api: any) {
  // gateway method: 外部から現在設定を確認するための軽量ステータス。
  api.registerGatewayMethod(`${pluginId}.status`, ({ respond }: any) => {
    try {
      const cfg = getCfg(api);
      respond(true, { ok: true, pluginId, configured: cfg });
    } catch (e: any) {
      respond(false, {
        ok: false,
        pluginId,
        error: String(e?.message || e),
      });
    }
  });

  registerToolRun(api, getCfg);
  registerToolBackfill(api, getCfg);
  registerToolDedup(api, getCfg);
  registerToolRelocate(api, getCfg);
  registerToolNormalizeFolderCase(api, getCfg);
  registerToolPrepareRelocateMetadata(api, getCfg);
  registerToolStatus(api, getCfg);
  registerToolValidate(api, getCfg);
  registerToolApplyReviewedMetadata(api, getCfg);
  registerToolDbBackup(api, getCfg);
  registerToolDbRestore(api, getCfg);
  registerToolRepairDb(api, getCfg);
  registerToolUpdateProgramTitles(api, getCfg);
  registerToolReextract(api, getCfg);
  registerToolLlmExtract(api, getCfg);
  registerToolLlmExtractStatus(api, getCfg);
  registerToolExportProgramYaml(api, getCfg);
  registerToolIngestEpg(api, getCfg);
  registerToolDetectFolderContamination(api, getCfg);
  registerToolDetectRebroadcasts(api, getCfg);
  registerToolLogs(api, getCfg);
  registerPluginHooks(api, getCfg);
  registerCli(api, pluginId, getCfg);
}
