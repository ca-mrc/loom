import { queryKeys } from "../api/queryKeys";
/**
 * Submit-trial dialog. Plan 25 redesign: the agent + model values
 * are pulled from server-side catalogs (`/agents`, `/models`) and
 * presented as dropdowns by AgentModelPicker. The model dropdown
 * is hidden when the selected agent doesn't call an LLM (oracle,
 * in-box runtimes), and the form rejects a half-filled "needs-a-
 * model" state client-side rather than letting the server 400.
 */
import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { useNavigate } from "react-router-dom";

import { api } from "../api";
import {
  AgentModelPicker,
  type AgentModelValue,
} from "./AgentModelPicker";
import {
  buildAgentModel,
  buildProviderOverride,
} from "./agentModelSelection";
import { findAgent } from "./agentModelPickerState";
import { Button } from "./Button";
import ErrorState from "./ErrorState";
import {
  agentReadinessMessage,
  agentServiceModeReady,
} from "../lib/agentReadiness";
import { Modal } from "./Modal";

export interface SubmitTrialModalProps {
  taskId: string;
  open: boolean;
  onClose: () => void;
}

const INITIAL_VALUE: AgentModelValue = {
  agentName: "",
  source: "api",
  modelProvider: "",
  modelName: "",
  hfExecution: "local-vllm",
};

export function SubmitTrialModal({
  taskId,
  open,
  onClose,
}: SubmitTrialModalProps): JSX.Element {
  const navigate = useNavigate();
  const [value, setValue] = useState<AgentModelValue>(INITIAL_VALUE);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<unknown>(null);

  const agents = useQuery({
    queryKey: queryKeys["agents"](),
    queryFn: () => api.listAgents(),
    staleTime: 5 * 60 * 1000,
  });

  const handleSubmit = async (): Promise<void> => {
    setError(null);
    const selected = findAgent(agents.data?.items, value.agentName);
    if (!selected) {
      setError(new Error("Pick an agent before submitting."));
      return;
    }
    if (!agentServiceModeReady(selected)) {
      setError(new Error(agentReadinessMessage(selected)));
      return;
    }
    const agentModel = buildAgentModel(value, selected.needs_model);
    const agentLabel = selected.display_name ?? selected.name;
    if (selected.needs_model && !value.providerConnectionId) {
      setError(new Error(`${agentLabel} needs a model — choose a provider connection first.`));
      return;
    }
    if (selected.needs_model && agentModel === null) {
      setError(new Error(`${agentLabel} needs a model — choose one from the dropdown.`));
      return;
    }
    const providerOverride = buildProviderOverride(
      value, selected.needs_model,
    );
    setSubmitting(true);
    try {
      if (providerOverride?.manual_model) {
        await api.addProviderConnectionModel(
          providerOverride.provider_connection_id,
          { model_id: providerOverride.provider_model_id },
        );
      }
      const result = await api.submitTrial({
        task_id: taskId,
        config: { agent_name: selected.name, agent_model: agentModel },
        ...(providerOverride
          ? {
              provider_connection_id: providerOverride.provider_connection_id,
              provider_model_id: providerOverride.provider_model_id,
            }
          : {}),
      });
      onClose();
      navigate(`/trials/${result.trial_id}`);
    } catch (e) {
      setError(e);
    } finally {
      setSubmitting(false);
    }
  };

  const reset = (): void => {
    setError(null);
    setValue(INITIAL_VALUE);
  };

  return (
    <Modal
      open={open}
      onClose={() => {
        reset();
        onClose();
      }}
      title="Submit trial"
      description="Pick the agent + model that will run this task."
      size="sm"
      footer={
        <>
          <Button
            variant="secondary"
            onClick={() => {
              reset();
              onClose();
            }}
            disabled={submitting}
          >
            Cancel
          </Button>
          <Button
            variant="primary"
            onClick={handleSubmit}
            disabled={submitting || agents.isPending}
          >
            {submitting ? "Submitting…" : "Submit trial"}
          </Button>
        </>
      }
    >
      <div className="space-y-4">
        {error ? <ErrorState error={error} /> : null}
        <p className="text-sm text-slate-600">
          A new trial will be queued for{" "}
          <code className="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-xs text-slate-700">
            {taskId}
          </code>
          .
        </p>
        <AgentModelPicker
          value={value}
          onChange={setValue}
          disabled={submitting}
        />
      </div>
    </Modal>
  );
}
