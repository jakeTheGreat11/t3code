import { assert, it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import {
  makeContinuationEmitter,
  continuationEmitterIdentity,
  ProviderContinuationRequests,
  layer,
  type ProviderContinuationRequest,
} from "./ProviderContinuationRequests.ts";

const instance = ProviderInstanceId.make("configured-provider");
const session = ProviderSessionId.make("logical-session");
const request: ProviderContinuationRequest = {
  threadId: ThreadId.make("thread"),
  providerThreadId: ProviderThreadId.make("provider-thread"),
  driver: ProviderDriverKind.make("test"),
  detail: null,
};

it.effect(
  "separates configured instance generations, parallel sessions and reopened logical sessions",
  () =>
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<ProviderContinuationRequest>();
      const sink = {
        offer: (value: ProviderContinuationRequest) =>
          Queue.offer(queue, value).pipe(Effect.asVoid),
      };
      const adapter = yield* makeContinuationEmitter(instance, sink);
      const original = yield* adapter.forSession(session);
      const parallel = yield* adapter.forSession(ProviderSessionId.make("parallel-session"));
      const reopened = yield* adapter.forSession(session);
      const replacement = yield* (yield* makeContinuationEmitter(instance, sink)).forSession(
        session,
      );
      assert.equal(original.identity.type, "known");
      if (
        original.identity.type !== "known" ||
        parallel.identity.type !== "known" ||
        reopened.identity.type !== "known" ||
        replacement.identity.type !== "known"
      )
        return yield* Effect.die("Expected known construction identities");
      assert.equal(original.identity.providerInstanceId, instance);
      assert.equal(original.identity.providerSessionId, session);
      assert.equal(
        original.identity.providerInstanceGenerationId,
        parallel.identity.providerInstanceGenerationId,
      );
      assert.equal(
        original.identity.providerInstanceGenerationId,
        reopened.identity.providerInstanceGenerationId,
      );
      assert.notEqual(
        original.identity.providerSessionGenerationId,
        parallel.identity.providerSessionGenerationId,
      );
      assert.notEqual(
        original.identity.providerSessionGenerationId,
        reopened.identity.providerSessionGenerationId,
      );
      assert.notEqual(
        original.identity.providerInstanceGenerationId,
        replacement.identity.providerInstanceGenerationId,
      );
      assert.notEqual(
        original.identity.providerSessionGenerationId,
        replacement.identity.providerSessionGenerationId,
      );
      // A delayed closure from the old runtime still emits the old identity.
      yield* original.offer(request);
      assert.deepEqual((yield* Queue.take(queue)).emitterIdentity, original.identity);
    }),
);

it.effect("captures identity before delayed offers and retains it through a queue retry", () =>
  Effect.gen(function* () {
    const requests = yield* ProviderContinuationRequests;
    const factory = yield* makeContinuationEmitter(instance, requests);
    const original = yield* factory.forSession(session);
    const delayed = original.offer({
      ...request,
      emitterIdentity: { type: "unknown", reason: "identity_unavailable" },
    });
    const later = yield* factory.forSession(session);
    yield* delayed;
    const first = yield* requests.take;
    assert.deepEqual(first.emitterIdentity, original.identity);
    assert.notDeepEqual(first.emitterIdentity, later.identity);
    assert.isTrue(Object.isFrozen(first.emitterIdentity));
    yield* requests.offer(first);
    const retried = yield* requests.take;
    assert.deepEqual(retried.emitterIdentity, original.identity);
    assert.equal(retried.threadId, request.threadId);
    assert.equal(retried.providerThreadId, request.providerThreadId);
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "routing changes and caller-supplied identity cannot replace runtime construction identity",
  () =>
    Effect.gen(function* () {
      const requests = yield* ProviderContinuationRequests;
      const factory = yield* makeContinuationEmitter(instance, requests);
      const original = yield* factory.forSession(session);
      const other = yield* factory.forSession(ProviderSessionId.make("other-session"));
      yield* original.offer({
        ...request,
        threadId: ThreadId.make("later-manual-thread"),
        providerThreadId: ProviderThreadId.make("later-routing"),
        emitterIdentity: other.identity,
      });
      const received = yield* requests.take;
      assert.deepEqual(received.emitterIdentity, original.identity);
      assert.equal(received.threadId, "later-manual-thread");
      assert.equal(received.providerThreadId, "later-routing");
    }).pipe(Effect.provide(layer)),
);

it.effect("legacy and app-owned requests retain explicit unknown identities", () =>
  Effect.gen(function* () {
    const requests = yield* ProviderContinuationRequests;
    assert.deepEqual(continuationEmitterIdentity(request), {
      type: "unknown",
      reason: "legacy_request",
    });
    yield* requests.offer(request);
    assert.deepEqual((yield* requests.take).emitterIdentity, {
      type: "unknown",
      reason: "legacy_request",
    });
    const appOwned: ProviderContinuationRequest = {
      ...request,
      emitterIdentity: { type: "unknown", reason: "not_runtime_owned" },
    };
    yield* requests.offer(appOwned);
    const received = yield* requests.take;
    yield* requests.offer(received);
    assert.deepEqual((yield* requests.take).emitterIdentity, appOwned.emitterIdentity);
  }).pipe(Effect.provide(layer)),
);

it.effect("keeps sink-free adapters inert and does not change callbacks or request content", () =>
  Effect.gen(function* () {
    const inert = yield* (yield* makeContinuationEmitter(instance)).forSession(session);
    yield* inert.offer(request);
    const requests = yield* ProviderContinuationRequests;
    const bound = yield* (yield* makeContinuationEmitter(instance, requests)).forSession(session);
    let cleared = 0;
    const clearIfCurrent = () =>
      Effect.sync(() => {
        cleared += 1;
      });
    yield* bound.offer({ ...request, detail: "existing result", clearIfCurrent });
    const received = yield* requests.take;
    assert.equal(received.detail, "existing result");
    assert.strictEqual(received.clearIfCurrent, clearIfCurrent);
    assert.equal(cleared, 0);
  }).pipe(Effect.provide(layer)),
);
