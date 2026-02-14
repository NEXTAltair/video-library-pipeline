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
