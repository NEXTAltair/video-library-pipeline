import { definePluginEntry } from "openclaw/plugin-sdk/plugin-entry";
import { registerCli } from "./src/plugin/cli";
import { pluginId, getCfg } from "./src/plugin/plugin-meta";
import { registerPluginHooks } from "./src/plugin/plugin-hooks";
import { registerWorkflowTools } from "./src/tools/tool-workflows";

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

    registerWorkflowTools(api, getCfg);
    registerPluginHooks(api, getCfg);
    registerCli(api, pluginId, getCfg);
  },
});
