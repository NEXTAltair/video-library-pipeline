type JsonObject = Record<string, unknown>;

export interface WorkflowNextAction {
  action: string;
  label?: string;
  tool?: string | null;
  params?: JsonObject;
  requiresHumanInput?: boolean;
}

export interface WorkflowResultPayload {
  ok: boolean;
  runId: string;
  flow: string;
  phase: string;
  outcome: string;
  artifacts?: JsonObject[];
  gates?: JsonObject[];
  nextActions?: WorkflowNextAction[];
  diagnostics?: JsonObject[];
  [key: string]: unknown;
}

export interface WorkflowFollowUpToolCall {
  tool: string;
  reason: string;
  params: JsonObject;
  requiresHumanReview: boolean;
}

export interface TranslatedWorkflowResult extends WorkflowResultPayload {
  artifacts: JsonObject[];
  gates: JsonObject[];
  nextActions: WorkflowNextAction[];
  diagnostics: JsonObject[];
  followUpToolCalls: WorkflowFollowUpToolCall[];
  hasFollowUpToolCalls: boolean;
  nextStep?: string;
}

function arrayOrEmpty<T>(value: unknown): T[] {
  return Array.isArray(value) ? (value as T[]) : [];
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function paramsOrEmpty(value: unknown): JsonObject {
  return value && typeof value === "object" && !Array.isArray(value)
    ? { ...(value as JsonObject) }
    : {};
}

function toFollowUpToolCall(action: WorkflowNextAction): WorkflowFollowUpToolCall | null {
  if (!isNonEmptyString(action.tool)) return null;
  if (!isNonEmptyString(action.action)) return null;

  return {
    tool: action.tool,
    reason: action.action,
    params: paramsOrEmpty(action.params),
    requiresHumanReview: action.requiresHumanInput === true,
  };
}

function nextStepFor(followUpToolCalls: WorkflowFollowUpToolCall[]): string | undefined {
  if (followUpToolCalls.length === 0) return undefined;
  if (followUpToolCalls.some((call) => call.requiresHumanReview)) {
    return "Review the referenced workflow artifacts with the user, then execute followUpToolCalls when approved.";
  }
  return "Proceed with followUpToolCalls to continue the workflow.";
}

export function translateWorkflowResult(payload: WorkflowResultPayload): TranslatedWorkflowResult {
  const nextActions = arrayOrEmpty<WorkflowNextAction>(payload.nextActions);
  const followUpToolCalls = nextActions
    .map((action) => toFollowUpToolCall(action))
    .filter((call): call is WorkflowFollowUpToolCall => call !== null);
  const nextStep = nextStepFor(followUpToolCalls);

  return {
    ...payload,
    artifacts: arrayOrEmpty<JsonObject>(payload.artifacts),
    gates: arrayOrEmpty<JsonObject>(payload.gates),
    nextActions,
    diagnostics: arrayOrEmpty<JsonObject>(payload.diagnostics),
    followUpToolCalls,
    hasFollowUpToolCalls: followUpToolCalls.length > 0,
    ...(nextStep ? { nextStep } : {}),
  };
}
