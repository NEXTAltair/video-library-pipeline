import { resolveConfig } from "./config";

// plugin 固有ID。設定キーや gateway method 名の接頭辞として使う。
export const pluginId = "video-library-pipeline";

// OpenClaw設定から plugin config を取り出して正規化する。
export function getCfg(api: any) {
  const raw = api?.config?.plugins?.entries?.[pluginId]?.config ?? {};
  return resolveConfig(raw);
}
