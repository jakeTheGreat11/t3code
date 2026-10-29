import * as Schema from "effect/Schema";
import { ProviderSessionId, TrimmedNonEmptyString } from "./baseSchemas.ts";
import { ProviderInstanceId } from "./providerInstance.ts";

export const ProviderInstanceGenerationId = TrimmedNonEmptyString.pipe(
  Schema.brand("ProviderInstanceGenerationId"),
);
export type ProviderInstanceGenerationId = typeof ProviderInstanceGenerationId.Type;
export const ProviderSessionGenerationId = TrimmedNonEmptyString.pipe(
  Schema.brand("ProviderSessionGenerationId"),
);
export type ProviderSessionGenerationId = typeof ProviderSessionGenerationId.Type;
/** T3 adapter/runtime identity, not a native process ID, run attribution, or submission permission. */
export const ContinuationEmitterIdentity = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("known"),
    providerInstanceId: ProviderInstanceId,
    providerInstanceGenerationId: ProviderInstanceGenerationId,
    providerSessionId: ProviderSessionId,
    providerSessionGenerationId: ProviderSessionGenerationId,
  }),
  Schema.Struct({
    type: Schema.Literal("unknown"),
    reason: Schema.Literals(["legacy_request", "not_runtime_owned", "identity_unavailable"]),
  }),
]);
export type ContinuationEmitterIdentity = typeof ContinuationEmitterIdentity.Type;
