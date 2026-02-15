import { registerCli } from "./src/cli";
import { pluginId, getCfg } from "./src/plugin-meta";
import { registerPluginHooks } from "./src/plugin-hooks";
import { registerToolLogs } from "./src/tool-logs";
import { registerToolReextract } from "./src/tool-reextract";
import { registerToolRepairDb } from "./src/tool-repair-db";
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
  registerToolStatus(api, getCfg);
  registerToolValidate(api, getCfg);
  registerToolRepairDb(api, getCfg);
  registerToolReextract(api, getCfg);
  registerToolLogs(api, getCfg);
  registerPluginHooks(api, getCfg);
  registerCli(api, pluginId, getCfg);
}
