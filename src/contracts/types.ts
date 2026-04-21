export type AnyObj = Record<string, any>;

export type CmdResult = {
  ok: boolean;
  code: number;
  stdout: string;
  stderr: string;
  command: string;
  args: string[];
  cwd?: string;
};

export type ToolDef = {
  name: string;
  description: string;
  parameters: AnyObj;
};

/** OpenClaw plugin API の最小型定義。SDK 本体の型が利用可能になれば置換する。 */
export type PluginApi = {
  registerTool(def: ToolDef & { execute: (id: string, params: AnyObj) => Promise<unknown> }): void;
  registerGatewayMethod?(name: string, handler: unknown): void;
  registerCli?(setup: unknown, opts?: unknown): void;
  on(event: string, handler: unknown, opts?: { priority?: number }): void;
  config?: { plugins?: { entries?: Record<string, { config?: AnyObj }> } };
  logger?: { info?(...args: unknown[]): void; warn?(...args: unknown[]): void };
};

export type GetCfgFn = (api: PluginApi) => import("./config").VideoPipelineResolvedConfig;
