import { memo } from "react";
import { NetworkIcon } from "lucide-react";
import type {
  EnvironmentId,
  ModelSelection,
  MultiAgentConfig,
  ProviderInstanceId,
} from "@t3tools/contracts";
import { createModelSelection, getProviderOptionCurrentValue } from "@t3tools/shared/model";
import { useComposerDraftStore, type ComposerThreadTarget } from "../../composerDraftStore";
import { isProviderInstancePickerReady, type ProviderInstanceEntry } from "../../providerInstances";
import { Button } from "../ui/button";
import {
  Dialog,
  DialogPopup,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogPanel,
} from "../ui/dialog";
import { Select, SelectTrigger, SelectValue, SelectPopup, SelectItem } from "../ui/select";
import {
  ComposerControl,
  ComposerControlChevron,
  ComposerControlIcon,
  type ComposerControlSize,
} from "./ComposerControl";
import { ProviderModelPicker } from "./ProviderModelPicker";
import type { ModelEsque } from "./providerIconUtils";
import { ComposerHandleContext } from "../../composerHandleContext";
import { composerFloatingLayerProps } from "./composerEventScope";
import {
  changeMultiAgentMode,
  clearMultiAgentRole,
  multiAgentControlLabel,
  multiAgentRoleDescriptors,
  MULTI_AGENT_EXECUTION_NOTICE,
  MULTI_AGENT_ROLES,
  setMultiAgentRoleModel,
  setMultiAgentRoleOption,
} from "./MultiAgentControl.logic";

export const MultiAgentControl = memo(function MultiAgentControl(props: {
  config: MultiAgentConfig;
  size: ComposerControlSize;
  disabled?: boolean;
  onOpen: () => void;
}) {
  return (
    <ComposerControl
      size={props.size}
      disabled={props.disabled}
      onClick={props.onOpen}
      aria-label={`${multiAgentControlLabel(props.config)}. Configure Multi-Agent preferences.`}
      aria-haspopup="dialog"
      title={MULTI_AGENT_EXECUTION_NOTICE}
    >
      <ComposerControlIcon icon={NetworkIcon} size={props.size} />
      <span data-composer-control-label>{multiAgentControlLabel(props.config)}</span>
      <ComposerControlChevron size={props.size} />
    </ComposerControl>
  );
});

export const MultiAgentPreferencesDialog = memo(function MultiAgentPreferencesDialog(props: {
  target: ComposerThreadTarget;
  environmentId: EnvironmentId | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  fallbackSelection: ModelSelection;
  instanceEntries: ReadonlyArray<ProviderInstanceEntry>;
  modelOptionsByInstance: ReadonlyMap<ProviderInstanceId, ReadonlyArray<ModelEsque>>;
}) {
  const config = useComposerDraftStore((store) => store.getMultiAgentConfig(props.target));
  const setConfig = useComposerDraftStore((store) => store.setMultiAgentConfig);
  const update = (transform: (current: MultiAgentConfig) => MultiAgentConfig) =>
    setConfig(
      props.target,
      transform(useComposerDraftStore.getState().getMultiAgentConfig(props.target)),
    );
  const disabledReason = (instanceId: ProviderInstanceId, model: string) => {
    const entry = props.instanceEntries.find((candidate) => candidate.instanceId === instanceId);
    return entry &&
      isProviderInstancePickerReady(entry) &&
      props.modelOptionsByInstance
        .get(instanceId)
        ?.some(
          (candidate) =>
            (candidate.slug === model || candidate.aliases?.includes(model)) &&
            !candidate.isUnavailable,
        )
      ? null
      : "This model is unavailable in this environment.";
  };
  return (
    <ComposerHandleContext value={null}>
      <Dialog open={props.open} onOpenChange={props.onOpenChange}>
        <DialogPopup {...composerFloatingLayerProps}>
          <DialogHeader>
            <DialogTitle>Multi-Agent preferences</DialogTitle>
            <DialogDescription>{MULTI_AGENT_EXECUTION_NOTICE}</DialogDescription>
          </DialogHeader>
          <DialogPanel>
            <div className="space-y-1.5">
              <span className="text-sm font-medium">Mode</span>
              <Select
                value={config.mode}
                onValueChange={(mode) => {
                  if (mode === "off" || mode === "auto" || mode === "custom")
                    update((current) => changeMultiAgentMode(current, mode));
                }}
              >
                <SelectTrigger size="sm" aria-label="Multi-Agent mode">
                  <SelectValue />
                </SelectTrigger>
                <SelectPopup {...composerFloatingLayerProps}>
                  <SelectItem value="off">Off</SelectItem>
                  <SelectItem value="auto">Auto</SelectItem>
                  <SelectItem value="custom">Custom</SelectItem>
                </SelectPopup>
              </Select>
            </div>
            {config.mode === "auto" ? (
              <div className="space-y-1.5">
                <span className="text-sm font-medium">Auto preset</span>
                <Select
                  value={config.preset}
                  onValueChange={(preset) => {
                    if (preset === "budget" || preset === "balanced" || preset === "maximum")
                      setConfig(props.target, { mode: "auto", preset });
                  }}
                >
                  <SelectTrigger size="sm" aria-label="Multi-Agent auto preset">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectPopup {...composerFloatingLayerProps}>
                    <SelectItem value="budget">Budget</SelectItem>
                    <SelectItem value="balanced">Balanced</SelectItem>
                    <SelectItem value="maximum">Maximum</SelectItem>
                  </SelectPopup>
                </Select>
              </div>
            ) : null}
            {config.mode === "custom"
              ? MULTI_AGENT_ROLES.map((role) => {
                  const selection = config.roles[role];
                  const roleLabel = role[0]!.toUpperCase() + role.slice(1);
                  const entry = props.instanceEntries.find(
                    (candidate) => candidate.instanceId === selection?.instanceId,
                  );
                  const unavailable =
                    selection !== undefined &&
                    disabledReason(selection.instanceId, selection.model) !== null;
                  const modelLabel = selection
                    ? (props.modelOptionsByInstance
                        .get(selection.instanceId)
                        ?.find(
                          (model) =>
                            model.slug === selection.model ||
                            model.aliases?.includes(selection.model),
                        )?.name ?? selection.model)
                    : "Unassigned";
                  const descriptors =
                    selection && !unavailable ? multiAgentRoleDescriptors(selection, entry) : [];
                  return (
                    <section
                      key={role}
                      aria-label={`${roleLabel} assignment`}
                      className="space-y-2 border-t pt-3"
                    >
                      <h3 className="text-sm font-medium">{roleLabel}</h3>
                      <div className="flex min-w-0 items-center justify-between gap-2">
                        <ProviderModelPicker
                          isComposerOwned
                          environmentId={props.environmentId}
                          activeInstanceId={
                            selection?.instanceId ?? props.fallbackSelection.instanceId
                          }
                          model={selection?.model ?? props.fallbackSelection.model}
                          lockedProvider={null}
                          instanceEntries={props.instanceEntries}
                          modelOptionsByInstance={props.modelOptionsByInstance}
                          triggerLabel={`${modelLabel}${unavailable ? " (Unavailable)" : ""}`}
                          triggerAriaLabel={`Choose ${role} model`}
                          disabled={!props.instanceEntries.some(isProviderInstancePickerReady)}
                          getModelDisabledReason={disabledReason}
                          onInstanceModelChange={(instanceId, model) => {
                            if (disabledReason(instanceId, model) === null)
                              update((current) =>
                                setMultiAgentRoleModel(current, role, instanceId, model),
                              );
                          }}
                        />
                        {selection ? (
                          <Button
                            variant="ghost"
                            size="sm"
                            aria-label={`Unassign ${role}`}
                            onClick={() => update((current) => clearMultiAgentRole(current, role))}
                          >
                            Unassign
                          </Button>
                        ) : null}
                      </div>
                      {selection ? (
                        <p className="text-xs text-muted-foreground">
                          {entry?.displayName ?? selection.instanceId}
                        </p>
                      ) : (
                        <p className="text-xs text-muted-foreground">No model assigned.</p>
                      )}
                      {unavailable ? (
                        <p className="text-xs text-muted-foreground">
                          Saved assignment retained. Choose an available model to edit its options.
                        </p>
                      ) : null}
                      {descriptors.map((descriptor) => {
                        const currentValue = getProviderOptionCurrentValue(descriptor);
                        const value =
                          descriptor.type === "boolean"
                            ? currentValue === undefined
                              ? null
                              : currentValue
                                ? "on"
                                : "off"
                            : typeof currentValue === "string" &&
                                descriptor.options.some((option) => option.id === currentValue)
                              ? currentValue
                              : null;
                        return (
                          <div key={descriptor.id} className="space-y-1">
                            <span className="text-xs text-muted-foreground">
                              {descriptor.label}
                            </span>
                            <Select
                              value={value}
                              onValueChange={(next) => {
                                if (typeof next === "string")
                                  update((current) =>
                                    setMultiAgentRoleOption(
                                      current,
                                      role,
                                      descriptor,
                                      descriptor.type === "boolean" ? next === "on" : next,
                                    ),
                                  );
                              }}
                            >
                              <SelectTrigger
                                size="sm"
                                aria-label={`${roleLabel} ${descriptor.label}`}
                              >
                                <SelectValue placeholder="Provider default" />
                              </SelectTrigger>
                              <SelectPopup {...composerFloatingLayerProps}>
                                {descriptor.type === "boolean" ? (
                                  <>
                                    <SelectItem value="on">On</SelectItem>
                                    <SelectItem value="off">Off</SelectItem>
                                  </>
                                ) : (
                                  descriptor.options.map((option) => (
                                    <SelectItem key={option.id} value={option.id}>
                                      {option.label}
                                      {option.isDefault ? " (default)" : ""}
                                    </SelectItem>
                                  ))
                                )}
                              </SelectPopup>
                            </Select>
                          </div>
                        );
                      })}
                      {selection?.options?.length ? (
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() =>
                            update((current) =>
                              current.mode === "custom" && current.roles[role]
                                ? {
                                    ...current,
                                    roles: {
                                      ...current.roles,
                                      [role]: createModelSelection(
                                        current.roles[role].instanceId,
                                        current.roles[role].model,
                                      ),
                                    },
                                  }
                                : current,
                            )
                          }
                        >
                          Reset {role} options
                        </Button>
                      ) : null}
                    </section>
                  );
                })
              : null}
          </DialogPanel>
        </DialogPopup>
      </Dialog>
    </ComposerHandleContext>
  );
});
