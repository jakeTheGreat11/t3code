import {
  ContinuationEmitterIdentity,
  type MultiAgentWorkerOwnership,
  type ProviderInstanceId,
  type ProviderSessionId,
} from "@t3tools/contracts";
import type { ProviderAdapterV2SessionRuntime } from "@t3tools/provider-core/server/ProviderAdapter";
import * as Schema from "effect/Schema";

type KnownIdentity = Extract<ContinuationEmitterIdentity, { type: "known" }>;
type Exposure = "unexposed" | "controlled_worker" | "unknown";
interface RuntimeRecord {
  readonly identity: KnownIdentity | undefined;
  readonly providerInstanceId: ProviderInstanceId;
  readonly providerSessionId: ProviderSessionId;
  active: boolean;
  conflicting: boolean;
  exposure: Exposure;
}
export interface ProviderRuntimeIdentityValidation {
  readonly status: "verified" | "retired" | "unknown" | "conflicting";
  readonly bindingExposure: Exposure;
}
const isEmitterIdentity = Schema.is(ContinuationEmitterIdentity);
const identitiesEqual = (a: KnownIdentity, b: KnownIdentity) =>
  a.providerInstanceId === b.providerInstanceId &&
  a.providerInstanceGenerationId === b.providerInstanceGenerationId &&
  a.providerSessionId === b.providerSessionId &&
  a.providerSessionGenerationId === b.providerSessionGenerationId;

/** Owned by one session manager. Only runtime construction registers identities;
 * offers can only read them. Retired generations remain tombstones. After process
 * loss, absent tombstones mean unknown, never evidence of non-exposure.
 */
export class ProviderRuntimeRegistry {
  private readonly runtimes = new WeakMap<ProviderAdapterV2SessionRuntime, RuntimeRecord>();
  private readonly generations = new Map<string, RuntimeRecord>();
  private readonly instanceGenerations = new Map<string, ProviderInstanceId>();
  private readonly conflictingInstances = new Set<string>();
  private readonly sessionHistory = new Map<string, Exposure>();

  register(input: {
    readonly runtime: ProviderAdapterV2SessionRuntime;
    readonly providerInstanceId: ProviderInstanceId;
    readonly providerSessionId: ProviderSessionId;
    readonly historicalState: "fresh" | "unknown";
  }): void {
    const supplied = input.runtime.continuationEmitterIdentity;
    const identity =
      supplied && isEmitterIdentity(supplied) && supplied.type === "known"
        ? Object.freeze({ ...supplied })
        : undefined;
    const registered = this.runtimes.get(input.runtime);
    if (registered) {
      if (
        registered.providerInstanceId !== input.providerInstanceId ||
        registered.providerSessionId !== input.providerSessionId ||
        input.runtime.instanceId !== registered.providerInstanceId ||
        input.runtime.providerSessionId !== registered.providerSessionId ||
        (registered.identity === undefined) !== (identity === undefined) ||
        (registered.identity && identity && !identitiesEqual(registered.identity, identity))
      ) {
        registered.conflicting = true;
        this.rememberHistory(registered);
      }
      return;
    }
    const bindingMatches =
      input.runtime.instanceId === input.providerInstanceId &&
      input.runtime.providerSessionId === input.providerSessionId &&
      (!identity ||
        (identity.providerInstanceId === input.providerInstanceId &&
          identity.providerSessionId === input.providerSessionId));
    const historyKey = JSON.stringify([input.providerInstanceId, input.providerSessionId]);
    const history = this.sessionHistory.get(historyKey);
    const record: RuntimeRecord = {
      identity,
      providerInstanceId: input.providerInstanceId,
      providerSessionId: input.providerSessionId,
      active: true,
      conflicting: !bindingMatches,
      exposure:
        history === "controlled_worker"
          ? history
          : identity && bindingMatches && input.historicalState === "fresh" && history !== "unknown"
            ? "unexposed"
            : "unknown",
    };
    this.runtimes.set(input.runtime, record);
    if (identity) {
      const previousInstance = this.instanceGenerations.get(identity.providerInstanceGenerationId);
      if (previousInstance !== undefined && previousInstance !== identity.providerInstanceId)
        this.conflictingInstances.add(identity.providerInstanceGenerationId);
      else
        this.instanceGenerations.set(
          identity.providerInstanceGenerationId,
          identity.providerInstanceId,
        );
      const previous = this.generations.get(identity.providerSessionGenerationId);
      if (previous) {
        // A second runtime object cannot reclaim a generation, even with identical metadata.
        previous.conflicting = true;
        record.conflicting = true;
      } else this.generations.set(identity.providerSessionGenerationId, record);
    }
    this.rememberHistory(record);
  }

  private rememberHistory(record: RuntimeRecord): void {
    const key = JSON.stringify([record.providerInstanceId, record.providerSessionId]);
    const previous = this.sessionHistory.get(key);
    this.sessionHistory.set(
      key,
      previous === "controlled_worker" || record.exposure === "controlled_worker"
        ? "controlled_worker"
        : previous === "unknown" || record.exposure === "unknown" || record.conflicting
          ? "unknown"
          : "unexposed",
    );
  }

  retire(runtime: ProviderAdapterV2SessionRuntime): void {
    const record = this.runtimes.get(runtime);
    if (record) record.active = false;
  }

  exposeControlledWorker(
    runtime: ProviderAdapterV2SessionRuntime,
    owner: MultiAgentWorkerOwnership,
  ):
    | "unregistered_runtime"
    | "retired_runtime"
    | "conflicting_identity"
    | "ownership_mismatch"
    | undefined {
    const record = this.runtimes.get(runtime);
    if (!record) return "unregistered_runtime";
    if (!record.active) return "retired_runtime";
    if (
      record.conflicting ||
      (record.identity &&
        this.conflictingInstances.has(record.identity.providerInstanceGenerationId))
    )
      return "conflicting_identity";
    if (
      record.providerInstanceId !== owner.providerInstanceId ||
      record.providerSessionId !== owner.providerSessionId
    )
      return "ownership_mismatch";
    record.exposure = "controlled_worker";
    this.rememberHistory(record);
    return undefined;
  }

  validate(identity: ContinuationEmitterIdentity | undefined): ProviderRuntimeIdentityValidation {
    if (!identity || !isEmitterIdentity(identity) || identity.type === "unknown")
      return { status: "unknown", bindingExposure: "unknown" };
    const record = this.generations.get(identity.providerSessionGenerationId);
    if (!record) return { status: "unknown", bindingExposure: "unknown" };
    if (
      !record.identity ||
      !identitiesEqual(identity, record.identity) ||
      record.conflicting ||
      this.conflictingInstances.has(identity.providerInstanceGenerationId)
    )
      return { status: "conflicting", bindingExposure: "unknown" };
    return {
      status: record.active ? "verified" : "retired",
      bindingExposure:
        !record.active && record.exposure === "unexposed" ? "unknown" : record.exposure,
    };
  }
}
