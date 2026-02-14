import type { ToolDef } from "./types";

// tool の仕様書(名前/説明/JSON Schema)を 1 箇所に集約する。
// 実際の execute 実装は index.ts 側で行う。
export const TOOL_DEFINITIONS: ToolDef[] = [
  {
    name: "video_pipeline_analyze_and_move_videos",
    description:
      "Analyze videos in source folder and move them to destination folder. Use apply=false for dry-run.",
    parameters: {
      type: "object",
      additionalProperties: false,
      properties: {
        apply: { type: "boolean", default: false },
        maxFilesPerRun: {
          type: "integer",
          minimum: 1,
          maximum: 5000,
          description: "Maximum files to process in one run for queue and plan stages.",
        },
        allowNeedsReview: { type: "boolean", default: false },
        profile: { type: "string" },
      },
    },
  },
  {
    name: "video_pipeline_status",
    description: "Read latest pipeline status summary from windowsOpsRoot/move.",
    parameters: {
      type: "object",
      additionalProperties: false,
      properties: {
        includeRawPaths: { type: "boolean", default: false },
      },
    },
  },
  {
    name: "video_pipeline_validate",
    description: "Validate config, binaries, and key path accessibility without side effects.",
    parameters: {
      type: "object",
      additionalProperties: false,
      properties: {
        checkWindowsInterop: { type: "boolean", default: true },
      },
    },
  },
  {
    name: "video_pipeline_repair_db",
    description: "Repair DB path fields from canonical Windows path values.",
    parameters: {
      type: "object",
      additionalProperties: false,
      properties: {
        dryRun: { type: "boolean", default: true },
        limit: { type: "integer", minimum: 1, maximum: 5000 },
      },
    },
  },
  {
    name: "video_pipeline_reextract",
    description: "Run metadata re-extraction batch from queue JSONL.",
    parameters: {
      type: "object",
      additionalProperties: false,
      properties: {
        queuePath: { type: "string" },
        extractionVersion: { type: "string" },
        batchSize: { type: "integer", minimum: 1, maximum: 1000, default: 50 },
        maxBatches: { type: "integer", minimum: 1, maximum: 1000 },
      },
    },
  },
  {
    name: "video_pipeline_logs",
    description: "Get latest log file pointers and optional tail text for video pipeline logs.",
    parameters: {
      type: "object",
      additionalProperties: false,
      properties: {
        kind: { type: "string", enum: ["apply", "plan", "inventory", "remaining", "all"], default: "all" },
        tail: { type: "integer", minimum: 1, maximum: 500, default: 50 },
      },
    },
  },
];
