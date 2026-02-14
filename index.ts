import { registerCli } from "./src/cli";
import { pluginId, getCfg } from "./src/plugin-meta";
import { registerToolLogs } from "./src/tool-logs";
import { registerToolReextract } from "./src/tool-reextract";
import { registerToolRepairDb } from "./src/tool-repair-db";
import { registerToolRun } from "./src/tool-run";
import { registerToolStatus } from "./src/tool-status";
import { registerToolValidate } from "./src/tool-validate";

export default function register(api: any) {
  // gateway method: 外部から現在設定を確認するための軽量ステータス。
  api.registerGatewayMethod(`${pluginId}.status`, ({ respond }: any) => {
    const cfg = getCfg(api);
    respond(true, { ok: true, pluginId, configured: cfg });
  });

  registerToolRun(api, getCfg);
  registerToolStatus(api, getCfg);
  registerToolValidate(api, getCfg);
  registerToolRepairDb(api, getCfg);
  registerToolReextract(api, getCfg);
  registerToolLogs(api, getCfg);
  registerCli(api, pluginId, getCfg);
}
