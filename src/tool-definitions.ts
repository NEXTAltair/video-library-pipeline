import type { ToolDef } from "./types";

// tool の仕様書(名前/説明/JSON Schema)を 1 箇所に集約する。
// 実際の execute 実装は index.ts 側で行う。
export const TOOL_DEFINITIONS: ToolDef[] = [
  {
    name: "video_pipeline_run",
    description:
      "Run end-to-end video pipeline (inventory, metadata, plan, apply, db update). Use apply=false for dry-run.",
    parameters: {
      type: "object",
      additionalProperties: false,
      properties: {
        apply: { type: "boolean", default: false },
        limit: { type: "integer", minimum: 1, maximum: 5000 },
        allowNeedsReview: { type: "boolean", default: false },
        profile: { type: "string" },
        pathsOverride: {
          type: "object",
          additionalProperties: false,
          properties: {
            db: { type: "string" },
            sourceRoot: { type: "string" },
            destRoot: { type: "string" },
            hostDataRoot: { type: "string" },
          },
        },
      },
    },
  },
  {
    name: "video_pipeline_status",
    description: "Read latest pipeline status from recent logs under hostDataRoot/move.",
    parameters: {
      type: "object",
      additionalProperties: false,
      properties: {
        windowHours: { type: "integer", minimum: 1, maximum: 168, default: 24 },
        includeLogs: { type: "boolean", default: true },
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
        strict: { type: "boolean", default: true },
        checkWindowsInterop: { type: "boolean", default: true },
      },
    },
  },
  {
    name: "video_pipeline_repair_db",
    description: "Run DB repair helpers for path/link consistency (safe/full modes).",
    parameters: {
      type: "object",
      additionalProperties: false,
      properties: {
        mode: { type: "string", enum: ["safe", "full"], default: "safe" },
        dryRun: { type: "boolean", default: true },
        limit: { type: "integer", minimum: 1, maximum: 5000 },
      },
    },
  },
  {
    name: "video_pipeline_reextract",
    description: "Run metadata re-extraction batch from queue source conditions.",
    parameters: {
      type: "object",
      additionalProperties: false,
      properties: {
        source: { type: "string", enum: ["needsReview", "version", "inventory"] },
        extractionVersion: { type: "string" },
        limit: { type: "integer", minimum: 1, maximum: 5000, default: 200 },
        batchSize: { type: "integer", minimum: 1, maximum: 1000, default: 50 },
      },
      required: ["source"],
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
