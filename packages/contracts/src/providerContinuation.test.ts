import { expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { ContinuationEmitterIdentity } from "./providerContinuation.ts";
const decode = Schema.decodeUnknownSync(ContinuationEmitterIdentity);
const encodeJson = Schema.encodeSync(Schema.fromJsonString(ContinuationEmitterIdentity));
const decodeJson = Schema.decodeSync(Schema.fromJsonString(ContinuationEmitterIdentity));
it("roundtrips independent instance and session generations", () => {
  const identity = decode({
    type: "known",
    providerInstanceId: "instance",
    providerInstanceGenerationId: "adapter-generation",
    providerSessionId: "session",
    providerSessionGenerationId: "runtime-generation",
  });
  expect(decodeJson(encodeJson(identity))).toEqual(identity);
});
it.each(["legacy_request", "not_runtime_owned", "identity_unavailable"])(
  "preserves explicit unknown reason %s",
  (reason) => {
    expect(decodeJson(encodeJson(decode({ type: "unknown", reason })))).toEqual({
      type: "unknown",
      reason,
    });
  },
);
it.each([
  {},
  { type: "known", providerInstanceId: "instance", providerSessionId: "session" },
  {
    type: "known",
    providerInstanceId: "instance",
    providerInstanceGenerationId: "",
    providerSessionId: "session",
    providerSessionGenerationId: "generation",
  },
  { type: "unknown" },
])("rejects incomplete emitter identity %j", (identity) => {
  expect(() => decode(identity)).toThrow();
});
