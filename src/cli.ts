export function registerCli(api: any, pluginId: string, getCfg: (api: any) => any) {
  // CLI補助コマンド: 現在の plugin config を確認する。
  api.registerCli?.(
    ({ program }: any) => {
      program
        .command("video-pipeline-status")
        .description("Show configured video-library-pipeline plugin values")
        .action(() => {
          const cfg = getCfg(api);
          console.log(JSON.stringify({ pluginId, config: cfg }, null, 2));
        });
    },
    { commands: ["video-pipeline-status"] },
  );
}
