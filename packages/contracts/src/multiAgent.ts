import * as Schema from "effect/Schema";
import { ModelSelection } from "./modelSelection.ts";

export const MultiAgentConfig = Schema.Union([
  Schema.Struct({ mode: Schema.Literal("off") }),
  Schema.Struct({
    mode: Schema.Literal("auto"),
    preset: Schema.Literals(["budget", "balanced", "maximum"]),
  }),
  Schema.Struct({
    mode: Schema.Literal("custom"),
    /** Unassigned roles may be saved while the user is editing preferences. */
    roles: Schema.Struct({
      planner: Schema.optional(ModelSelection),
      worker: Schema.optional(ModelSelection),
      rescuer: Schema.optional(ModelSelection),
      reviewer: Schema.optional(ModelSelection),
    }),
  }),
]);
export type MultiAgentConfig = typeof MultiAgentConfig.Type;

export const DEFAULT_MULTI_AGENT_CONFIG: MultiAgentConfig = { mode: "off" };
