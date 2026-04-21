import { definePluginEntry } from "openclaw/plugin-sdk/plugin-entry";
import { registerCli } from "./src/plugin/cli";
import { pluginId, getCfg } from "./src/plugin/plugin-meta";
import { registerPluginHooks } from "./src/plugin/plugin-hooks";
import { registerToolApplyReviewedMetadata } from "./src/tools/tool-apply-reviewed-metadata";
import { registerToolDbBackup, registerToolDbRestore } from "./src/tools/tool-db-backup";
import { registerToolBackfill } from "./src/tools/tool-backfill";
import { registerToolDedup } from "./src/tools/tool-dedup";
import { registerToolDedupBroadcasterReview } from "./src/tools/tool-dedup-broadcaster-review";
import { registerToolDedupDropReview } from "./src/tools/tool-dedup-drop-review";
import { registerToolDedupRebroadcasts } from "./src/tools/tool-dedup-rebroadcasts";
import { registerToolLogs } from "./src/tools/tool-logs";
import { registerToolExportProgramYaml } from "./src/tools/tool-export-program-yaml";
import { registerToolDetectFolderContamination } from "./src/tools/tool-detect-folder-contamination";
import { registerToolDetectRebroadcasts } from "./src/tools/tool-detect-rebroadcasts";
import { registerToolIngestEpg } from "./src/tools/tool-ingest-epg";
import { registerToolPrepareRelocateMetadata } from "./src/tools/tool-prepare-relocate-metadata";
import { registerToolRelocate } from "./src/tools/tool-relocate";
import { registerToolNormalizeFolderCase } from "./src/tools/tool-normalize-folder-case";
import { registerToolReextract } from "./src/tools/tool-reextract";
import { registerToolLlmExtract } from "./src/tools/tool-llm-extract";
import { registerToolLlmExtractStatus } from "./src/tools/tool-llm-extract-status";
import { registerToolRepairDb } from "./src/tools/tool-repair-db";
import { registerToolUpdateProgramTitles } from "./src/tools/tool-update-program-titles";
import { registerToolRun } from "./src/tools/tool-run";
import { registerToolStatus } from "./src/tools/tool-status";
import { registerToolValidate } from "./src/tools/tool-validate";

export default definePluginEntry({
  id: pluginId,
  name: "Video Library Pipeline",
  description:
    "Config-driven video library pipeline for inventory, metadata extraction, move planning, apply, and audit logging.",
  register(api: any) {
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
    registerToolDedupBroadcasterReview(api, getCfg);
    registerToolDedupDropReview(api, getCfg);
    registerToolDedupRebroadcasts(api, getCfg);
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
  },
});
