import type { PluginApi, GetCfgFn, AnyObj } from "./types";

// Integrity checks live in the workflow engine. Do not gate unrelated tools or
// inject Calibre policy into every conversation from a video plugin.
export function registerPluginHooks(api: PluginApi, _getCfg: GetCfgFn) {
  api.on("after_tool_call", (event: AnyObj) => {
    if (!String(event?.toolName ?? "").startsWith("video_pipeline_")) return;
    if (event.error) api.logger?.warn?.(`[video-pipeline] ${event.toolName}: ${event.error}`);
  });
}
