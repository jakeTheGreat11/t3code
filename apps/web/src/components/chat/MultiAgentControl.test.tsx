// @vitest-environment jsdom
import { act, useState, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import {
  EnvironmentId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type ServerProvider,
} from "@t3tools/contracts";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { useComposerDraftStore } from "../../composerDraftStore";
import { deriveProviderInstanceEntries } from "../../providerInstances";
import { MultiAgentControl, MultiAgentPreferencesDialog } from "./MultiAgentControl";
import { CompactComposerControlsMenu } from "./CompactComposerControlsMenu";
import { multiAgentControlLabel } from "./MultiAgentControl.logic";

vi.mock("@tanstack/react-router", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@tanstack/react-router")>()),
  useNavigate: () => vi.fn(),
}));

// jsdom has no list viewport measurements; keep the real picker rows and selection behavior.
vi.mock("@legendapp/list/react", () => ({
  LegendList: (props: {
    data: readonly string[];
    renderItem: (input: { item: string; index: number }) => ReactNode;
  }) => (
    <div>
      {props.data.map((item, index) => (
        <div key={item}>{props.renderItem({ item, index })}</div>
      ))}
    </div>
  ),
}));

const environmentId = EnvironmentId.make("preferences-environment");
const target = scopeThreadRef(environmentId, ThreadId.make("preferences-thread"));
const instanceId = ProviderInstanceId.make("configured-instance");
const provider: ServerProvider = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  displayName: "Configured provider",
  enabled: true,
  installed: true,
  status: "ready",
  version: null,
  auth: { status: "authenticated" },
  checkedAt: "2026-10-01T00:00:00Z",
  models: [
    {
      slug: "user-model",
      name: "User model",
      isCustom: true,
      capabilities: {
        optionDescriptors: [
          {
            id: "catalog-option",
            label: "Reasoning",
            type: "select",
            options: [
              { id: "economy", label: "Economy" },
              { id: "thorough", label: "Thorough" },
            ],
          },
        ],
      },
    },
  ],
  slashCommands: [],
  skills: [],
};
const entries = deriveProviderInstanceEntries([provider]);
const models = new Map([[instanceId, [{ slug: "user-model", name: "User model" }]]]);
let container: HTMLDivElement;
let root: Root;

function Harness({ compact = false }: { compact?: boolean }) {
  const [open, setOpen] = useState(false);
  const config = useComposerDraftStore((store) => store.getMultiAgentConfig(target));
  return (
    <>
      {compact ? (
        <CompactComposerControlsMenu
          interactionMode="default"
          runtimeMode="full-access"
          runtimeModeOptions={[]}
          showInteractionModeToggle={false}
          onToggleInteractionMode={() => {}}
          onRuntimeModeChange={() => {}}
          multiAgentLabel={multiAgentControlLabel(config)}
          onOpenMultiAgentPreferences={() => setOpen(true)}
        />
      ) : (
        <MultiAgentControl config={config} size="sm" onOpen={() => setOpen(true)} />
      )}
      <MultiAgentPreferencesDialog
        target={target}
        environmentId={environmentId}
        open={open}
        onOpenChange={setOpen}
        fallbackSelection={{ instanceId, model: "user-model" }}
        instanceEntries={entries}
        modelOptionsByInstance={models}
      />
    </>
  );
}

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  Object.defineProperty(Element.prototype, "getAnimations", {
    configurable: true,
    value: () => [],
  });
  vi.stubGlobal(
    "ResizeObserver",
    class {
      observe() {}
      unobserve() {}
      disconnect() {}
    },
  );
  useComposerDraftStore.setState({
    draftsByThreadKey: {},
    draftThreadsByThreadKey: {},
    logicalProjectDraftThreadKeyByLogicalProjectKey: {},
  });
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});
afterEach(async () => {
  await act(() => root.unmount());
  container.remove();
  Reflect.deleteProperty(Element.prototype, "getAnimations");
  vi.unstubAllGlobals();
});

function button(label: string) {
  const element = document.querySelector<HTMLButtonElement>(`button[aria-label="${label}"]`);
  if (!element) throw new Error(`Missing button: ${label}`);
  return element;
}
async function choose(label: string, text: string) {
  await act(() => button(label).click());
  const option = [...document.querySelectorAll<HTMLElement>('[role="option"]')].find(
    (element) => element.textContent === text,
  );
  if (!option) throw new Error(`Missing option: ${text}`);
  await act(() => option.click());
}

describe("Multi-Agent composer preferences UI", () => {
  it("selects a mode using the keyboard", async () => {
    await act(() => root.render(<Harness />));
    await act(() => container.querySelector<HTMLButtonElement>("button")!.click());
    const trigger = button("Multi-Agent mode");
    await act(() => {
      trigger.focus();
      trigger.dispatchEvent(new KeyboardEvent("keydown", { key: "ArrowDown", bubbles: true }));
    });
    await act(() =>
      document.activeElement!.dispatchEvent(
        new KeyboardEvent("keydown", { key: "ArrowDown", bubbles: true }),
      ),
    );
    await act(() =>
      document.activeElement!.dispatchEvent(
        new KeyboardEvent("keydown", { key: "Enter", bubbles: true }),
      ),
    );
    expect(useComposerDraftStore.getState().getMultiAgentConfig(target)).toEqual({
      mode: "auto",
      preset: "balanced",
    });
  });

  it("assigns a role through the existing model picker without changing the main model", async () => {
    useComposerDraftStore.getState().setMultiAgentConfig(target, { mode: "custom", roles: {} });
    await act(() => root.render(<Harness />));
    await act(() => container.querySelector<HTMLButtonElement>("button")!.click());
    await act(() => button("Choose planner model").click());
    const option = [...document.querySelectorAll<HTMLElement>('[role="option"]')].find((element) =>
      element.textContent?.includes("User model"),
    );
    expect(option).toBeDefined();
    await act(() => option!.click());
    const config = useComposerDraftStore.getState().getMultiAgentConfig(target);
    expect(config.mode === "custom" && config.roles.planner).toEqual({
      instanceId,
      model: "user-model",
    });
    expect(
      useComposerDraftStore.getState().getComposerDraft(target)?.modelSelectionByProvider,
    ).toEqual({});
    expect(document.querySelector('[role="dialog"]')?.textContent).toContain(
      "Multi-Agent preferences",
    );
  });

  it.each([false, true])(
    "edits Auto preferences from compact=%s and keeps Off dispatch settings intact",
    async (compact) => {
      await act(() => root.render(<Harness compact={compact} />));
      if (compact) {
        await act(() => button("More composer controls").click());
        const item = [...document.querySelectorAll<HTMLElement>('[role="menuitem"]')].find(
          (element) => element.textContent?.includes("Multi-Agent: Off"),
        );
        expect(item).toBeDefined();
        await act(() => item!.click());
      } else {
        await act(() => container.querySelector<HTMLButtonElement>("button")!.click());
      }
      expect(document.querySelector('[role="dialog"]')?.textContent).toContain(
        "Auto and Custom support Codex coordinators with Codex workers",
      );
      await choose("Multi-Agent mode", "Auto");
      await choose("Multi-Agent auto preset", "Maximum");
      expect(useComposerDraftStore.getState().getMultiAgentConfig(target)).toEqual({
        mode: "auto",
        preset: "maximum",
      });
      expect(
        useComposerDraftStore.getState().getComposerDraft(target)?.modelSelectionByProvider,
      ).toEqual({});
      await choose("Multi-Agent mode", "Off");
      expect(useComposerDraftStore.getState().getMultiAgentConfig(target)).toEqual({ mode: "off" });
      expect(useComposerDraftStore.getState().getComposerDraft(target)).toBeNull();
    },
  );

  it("shows unassigned roles and edits supported reasoning options without changing the prompt", async () => {
    useComposerDraftStore.getState().setPrompt(target, "Keep the ordinary prompt");
    useComposerDraftStore.getState().setMultiAgentConfig(target, {
      mode: "custom",
      roles: { worker: { instanceId, model: "user-model" } },
    });
    await act(() => root.render(<Harness />));
    await act(() => container.querySelector<HTMLButtonElement>("button")!.click());
    expect(document.querySelector('[aria-label="Planner assignment"]')?.textContent).toContain(
      "Unassigned",
    );
    await choose("Worker Reasoning", "Thorough");
    const config = useComposerDraftStore.getState().getMultiAgentConfig(target);
    expect(config.mode === "custom" && config.roles.worker?.options).toEqual([
      { id: "catalog-option", value: "thorough" },
    ]);
    expect(useComposerDraftStore.getState().getComposerDraft(target)?.prompt).toBe(
      "Keep the ordinary prompt",
    );
    await act(() => button("Unassign worker").click());
    expect(document.querySelector('[aria-label="Worker assignment"]')?.textContent).toContain(
      "Unassigned",
    );
  });

  it("retains unavailable custom assignments without offering invented options", async () => {
    useComposerDraftStore.getState().setMultiAgentConfig(target, {
      mode: "custom",
      roles: {
        reviewer: {
          instanceId,
          model: "missing-model",
          options: [{ id: "saved-option", value: "saved-value" }],
        },
      },
    });
    await act(() => root.render(<Harness />));
    await act(() => container.querySelector<HTMLButtonElement>("button")!.click());
    const role = document.querySelector('[aria-label="Reviewer assignment"]');
    expect(role?.textContent).toContain("missing-model (Unavailable)");
    expect(role?.textContent).toContain("Saved assignment retained");
    expect(role?.querySelectorAll('[role="combobox"]')).toHaveLength(0);
    expect(useComposerDraftStore.getState().getMultiAgentConfig(target)).toEqual({
      mode: "custom",
      roles: {
        reviewer: {
          instanceId,
          model: "missing-model",
          options: [{ id: "saved-option", value: "saved-value" }],
        },
      },
    });
  });
});
