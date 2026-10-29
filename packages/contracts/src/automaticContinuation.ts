import * as Schema from "effect/Schema";
import { ProviderSessionId, ProviderThreadId, RunId, ThreadId } from "./baseSchemas.ts";
import { ProviderInstanceId } from "./providerInstance.ts";

export const AutomaticTriggerRunReference = Schema.Struct({ threadId: ThreadId, runId: RunId });
export type AutomaticTriggerRunReference = typeof AutomaticTriggerRunReference.Type;
export const AutomaticTriggerProviderBinding = Schema.Struct({
  providerInstanceId: ProviderInstanceId,
  providerThreadId: ProviderThreadId,
  providerSessionId: ProviderSessionId,
});
export type AutomaticTriggerProviderBinding = typeof AutomaticTriggerProviderBinding.Type;
export const AutomaticTriggerKind = Schema.Literals([
  "provider_background",
  "delegated_completion",
  "steer_follow_up",
  "restart",
]);
export const AutomaticTriggerProvenance = Schema.Union([
  Schema.Struct({ type: Schema.Literal("exact"), originatingRun: AutomaticTriggerRunReference }),
  Schema.Struct({ type: Schema.Literal("unknown") }),
  // Candidates are diagnostic evidence, never a choice of which run to authorize.
  Schema.Struct({
    type: Schema.Literal("ambiguous"),
    candidates: Schema.optional(
      Schema.Array(AutomaticTriggerRunReference).check(Schema.isMaxLength(16)),
    ),
  }),
]);
export type AutomaticTriggerProvenance = typeof AutomaticTriggerProvenance.Type;
/** T3-owned classification, not permission or proof that a provider received a submission. */
export const NewRunTrigger = Schema.Union([
  Schema.Struct({ type: Schema.Literal("manual") }),
  Schema.Struct({ type: Schema.Literal("ordinary") }),
  Schema.Struct({
    type: Schema.Literal("automatic"),
    kind: AutomaticTriggerKind,
    provenance: AutomaticTriggerProvenance,
    providerBinding: Schema.optional(AutomaticTriggerProviderBinding),
  }),
]);
export type NewRunTrigger = typeof NewRunTrigger.Type;
