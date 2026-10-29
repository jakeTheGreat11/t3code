import { assert, it } from "@effect/vitest";
import {
  MULTI_AGENT_CONTROLLER_LIMITS,
  MultiAgentProposal,
  ThreadId,
  RunId,
  ProviderInstanceId,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";
import { MULTI_AGENT_COORDINATOR_INSTRUCTIONS } from "./MultiAgentCoordinatorInstructions.ts";
import { multiAgentCoordinatorInstructions } from "./MultiAgentCoordinatorInstructions.ts";
const decodeProposal = Schema.decodeUnknownEffect(Schema.fromJsonString(MultiAgentProposal));

it.effect("the instructed JSON example is accepted by the exact production proposal schema", () => {
  const example = MULTI_AGENT_COORDINATOR_INSTRUCTIONS.split("\n").find((line) =>
    line.startsWith('{"tasks":'),
  )!;
  assert.equal(
    example,
    '{"tasks":[{"key":"unique-task-key","role":"worker","prompt":"Clear independent task instructions"}]}',
  );
  return decodeProposal(example, {
    onExcessProperty: "error",
  }).pipe(
    Effect.map((proposal) => {
      assert.deepEqual(proposal, {
        tasks: [
          { key: "unique-task-key", role: "worker", prompt: "Clear independent task instructions" },
        ],
      });
    }),
  );
});

it("guidance reflects the controller limits without requiring a user-supplied plan", () => {
  const limits = MULTI_AGENT_CONTROLLER_LIMITS;
  assert.include(
    MULTI_AGENT_COORDINATOR_INSTRUCTIONS,
    `${limits.concurrentPerController} concurrent workers for this controller`,
  );
  assert.include(
    MULTI_AGENT_COORDINATOR_INSTRUCTIONS,
    `${limits.concurrentPerEnvironment} controlled workers across the environment`,
  );
  assert.include(
    MULTI_AGENT_COORDINATOR_INSTRUCTIONS,
    `${limits.totalTasksPerController} total tasks for this controller`,
  );
  assert.include(
    MULTI_AGENT_COORDINATOR_INSTRUCTIONS,
    "Never ask the user to choose an agent count, roles, or a task breakdown",
  );
});

it.each(["budget", "balanced", "maximum"] as const)(
  "Auto %s guidance uses only the run's frozen candidates and asks for task-aware effort",
  (preset) => {
    const instructions = multiAgentCoordinatorInstructions({
      multiAgent: { mode: "auto", preset },
      multiAgentController: {
        coordinatorThreadId: ThreadId.make("coordinator-thread"),
        coordinatorRunId: RunId.make("coordinator-run"),
        status: "open",
        tasks: [],
        autoRoutingCatalog: {
          providerInstanceId: ProviderInstanceId.make("account"),
          fallback: { instanceId: ProviderInstanceId.make("account"), model: "catalog-model" },
          candidates: [
            {
              id: "offered-candidate",
              model: {
                slug: "catalog-model",
                name: "Catalog model",
                isCustom: true,
                capabilities: null,
              },
            },
          ],
        },
      },
    });
    assert.include(instructions, `preset: ${preset}`);
    assert.include(instructions, "offered-candidate");
    assert.include(instructions, "evaluate each assignment");
    assert.include(instructions, "coding difficulty, ambiguity");
    assert.include(instructions, "every advertised select option");
    assert.notInclude(instructions, "applies the saved Custom model");
    assert.include(instructions, '"routing":{"candidateId"');
    assert.equal(instructions.split("T3 Code Multi-Agent coordinator instructions").length - 1, 1);
  },
);
