import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import { DEFAULT_MULTI_AGENT_CONFIG, MultiAgentConfig } from "./multiAgent.ts";
import {
  OrchestrationV2Command,
  OrchestrationV2RunJson,
  orchestrationV2RunMultiAgentConfig,
} from "./orchestrationV2.ts";

const decode = Schema.decodeUnknownSync(MultiAgentConfig);
const encode = Schema.encodeSync(MultiAgentConfig);
const decodeCommand = Schema.decodeUnknownSync(OrchestrationV2Command);
const decodeRun = Schema.decodeUnknownSync(OrchestrationV2RunJson);
const decodeEncodedRun = Schema.decodeSync(OrchestrationV2RunJson);
const encodeRun = Schema.encodeSync(OrchestrationV2RunJson);

it.each([
  undefined,
  { mode: "off" },
  { mode: "auto", preset: "balanced" },
  {
    mode: "custom",
    roles: {
      worker: {
        instanceId: "configured-worker",
        model: "catalog-model",
        options: [
          { id: "catalog-option", value: "catalog-value" },
          { id: "toggle", value: false },
        ],
      },
    },
  },
])("round-trips dispatch and durable run policy with legacy compatibility: %j", (multiAgent) => {
  const command = decodeCommand({
    type: "message.dispatch",
    commandId: "policy",
    threadId: "thread",
    messageId: "message",
    text: "Work",
    attachments: [],
    dispatchMode: { type: "start_immediately" },
    createdBy: "user",
    creationSource: "web",
    ...(multiAgent === undefined ? {} : { multiAgent }),
  });
  expect(command).toMatchObject(multiAgent === undefined ? {} : { multiAgent });
  const run = decodeRun({
    id: "run",
    threadId: "thread",
    ordinal: 1,
    providerInstanceId: "configured-coordinator",
    modelSelection: { instanceId: "configured-coordinator", model: "coordinator-model" },
    providerThreadId: null,
    userMessageId: "message",
    rootNodeId: null,
    activeAttemptId: null,
    status: "running",
    requestedAt: "2026-10-01T00:00:00Z",
    startedAt: null,
    completedAt: null,
    checkpointId: null,
    contextHandoffId: null,
    ...(multiAgent === undefined ? {} : { multiAgent }),
  });
  expect(orchestrationV2RunMultiAgentConfig(run)).toEqual(multiAgent ?? { mode: "off" });
  expect(decodeEncodedRun(encodeRun(run))).toEqual(run);
});

describe("MultiAgentConfig", () => {
  it("defaults to off", () => {
    expect(DEFAULT_MULTI_AGENT_CONFIG).toEqual({ mode: "off" });
    expect(decode(DEFAULT_MULTI_AGENT_CONFIG)).toEqual(DEFAULT_MULTI_AGENT_CONFIG);
  });

  it.each(["budget", "balanced", "maximum"])("round-trips the %s automatic preset", (preset) => {
    const config = decode({ mode: "auto", preset });
    expect(decode(encode(config))).toEqual({ mode: "auto", preset });
  });

  it("round-trips all custom roles with provider-specific options", () => {
    const selection = {
      instanceId: "configured-instance",
      model: "user-model",
      options: [
        { id: "provider-option", value: "provider-choice" },
        { id: "provider-toggle", value: true },
      ],
    };
    const config = decode({
      mode: "custom",
      roles: {
        planner: selection,
        worker: selection,
        rescuer: selection,
        reviewer: selection,
      },
    });
    expect(decode(encode(config))).toEqual(config);
  });

  it("accepts incomplete custom preferences without inventing selections", () => {
    expect(decode({ mode: "custom", roles: {} })).toEqual({ mode: "custom", roles: {} });
  });

  it("normalizes historical model-selection and option shapes", () => {
    expect(
      decode({
        mode: "custom",
        roles: {
          worker: {
            provider: "configured-instance",
            model: "user-model",
            options: { "provider-option": "choice" },
          },
        },
      }),
    ).toEqual({
      mode: "custom",
      roles: {
        worker: {
          instanceId: "configured-instance",
          model: "user-model",
          options: [{ id: "provider-option", value: "choice" }],
        },
      },
    });
  });

  it.each([
    null,
    "auto",
    {},
    { mode: "unknown" },
    { mode: "auto" },
    { mode: "auto", preset: "unknown" },
    { mode: "custom" },
    { mode: "custom", roles: { worker: { instanceId: "configured-instance" } } },
    { mode: "custom", roles: { reviewer: { instanceId: "configured-instance", model: "" } } },
  ])("rejects malformed configuration %j", (input) => {
    expect(() => decode(input)).toThrow();
  });
});
