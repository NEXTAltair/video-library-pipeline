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
    name: "video_pipeline_backfill_moved_files",
    description: "Backfill already-moved files into DB with dry-run/apply and optional metadata queue generation.",
    parameters: {
      type: "object",
      additionalProperties: false,
      properties: {
        apply: { type: "boolean", default: false },
        roots: { type: "array", items: { type: "string" } },
        rootsFilePath: { type: "string" },
        extensions: { type: "array", items: { type: "string" }, default: [".mp4"] },
        limit: { type: "integer", minimum: 1, maximum: 100000 },
        includeObservations: { type: "boolean", default: true },
        queueMissingMetadata: { type: "boolean", default: false },
        driveMap: { type: "object", additionalProperties: { type: "string" } },
        detectCorruption: { type: "boolean", default: true },
        corruptionReadBytes: { type: "integer", minimum: 1, maximum: 1048576, default: 4096 },
        scanErrorPolicy: { type: "string", enum: ["warn", "fail", "threshold"], default: "warn" },
        scanErrorThreshold: { type: "integer", minimum: 1, maximum: 100000 },
        scanRetryCount: { type: "integer", minimum: 0, maximum: 10, default: 1 },
      },
    },
  },
  {
    name: "video_pipeline_dedup_recordings",
    description: "Detect duplicate recordings and isolate drop candidates to quarantine (dry-run/apply).",
    parameters: {
      type: "object",
      additionalProperties: false,
      properties: {
        apply: { type: "boolean", default: false },
        maxGroups: { type: "integer", minimum: 1, maximum: 5000 },
        confidenceThreshold: { type: "number", minimum: 0, maximum: 1, default: 0.85 },
        allowNeedsReview: { type: "boolean", default: false },
        keepTerrestrialAndBscs: { type: "boolean", default: true },
        bucketRulesPath: { type: "string" },
      },
    },
  },
  {
    name: "video_pipeline_relocate_existing_files",
    description:
      "Relocate existing files under specified roots to current placement rules using DB metadata (dry-run/apply).",
    parameters: {
      type: "object",
      additionalProperties: false,
      properties: {
        apply: { type: "boolean", default: false },
        roots: { type: "array", items: { type: "string" } },
        rootsFilePath: { type: "string" },
        extensions: { type: "array", items: { type: "string" }, default: [".mp4"] },
        limit: { type: "integer", minimum: 1, maximum: 100000 },
        allowNeedsReview: { type: "boolean", default: false },
        queueMissingMetadata: { type: "boolean", default: false },
        writeMetadataQueueOnDryRun: { type: "boolean", default: false },
        scanErrorPolicy: { type: "string", enum: ["warn", "fail", "threshold"], default: "warn" },
        scanErrorThreshold: { type: "integer", minimum: 1, maximum: 100000 },
        scanRetryCount: { type: "integer", minimum: 0, maximum: 10, default: 1 },
        onDstExists: { type: "string", enum: ["error", "rename_suffix"], default: "error" },
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
    name: "video_pipeline_apply_reviewed_metadata",
    description: "Apply reviewed extracted metadata JSONL to DB and mark rows as human-reviewed.",
    parameters: {
      type: "object",
      additionalProperties: false,
      properties: {
        sourceJsonlPath: { type: "string" },
        outputStampedJsonlPath: { type: "string" },
        markHumanReviewed: { type: "boolean", default: true },
        reviewedBy: { type: "string" },
        source: { type: "string", default: "llm" },
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
        preserveHumanReviewed: { type: "boolean", default: true },
      },
    },
  },
  {
    name: "video_pipeline_export_program_yaml",
    description: "Export reviewed candidate program info YAML from extracted metadata JSONL.",
    parameters: {
      type: "object",
      additionalProperties: false,
      properties: {
        sourceJsonlPath: { type: "string" },
        outputPath: { type: "string" },
        includeNeedsReview: { type: "boolean", default: true },
        includeUnknown: { type: "boolean", default: false },
        maxSamplesPerProgram: { type: "integer", minimum: 1, maximum: 20, default: 3 },
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
        kind: {
          type: "string",
          enum: [
            "apply",
            "plan",
            "inventory",
            "remaining",
            "backfill-plan",
            "backfill-apply",
            "backfill-queue",
            "dedup-plan",
            "dedup-apply",
            "relocate-plan",
            "relocate-apply",
            "relocate-queue",
            "all",
          ],
          default: "all",
        },
        tail: { type: "integer", minimum: 1, maximum: 500, default: 50 },
      },
    },
  },
];
