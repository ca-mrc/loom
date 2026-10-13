import { useId, useState } from "react";

import { Button } from "../Button";
import { Input } from "../Input";
import { Modal } from "../Modal";

export type AddManualModelModalProps = {
  onClose: () => void;
  onSubmit: (model: { model_id: string; display_name?: string }) => void;
  pending?: boolean;
};

export default function AddManualModelModal({
  onClose, onSubmit, pending,
}: AddManualModelModalProps): JSX.Element {
  const [modelId, setModelId] = useState("");
  const [displayName, setDisplayName] = useState("");
  const modelIdInputId = useId();
  const displayNameInputId = useId();
  const canSubmit = modelId.trim().length > 0;

  return (
    <Modal
      open
      onClose={onClose}
      title="Add manual model"
      description="Manually register a model the upstream catalog doesn't expose. The model will appear in pickers immediately."
      size="sm"
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button
            variant="primary"
            onClick={() =>
              onSubmit({
                model_id: modelId.trim(),
                ...(displayName.trim()
                  ? { display_name: displayName.trim() }
                  : {}),
              })
            }
            disabled={!canSubmit || pending}
          >
            Add model
          </Button>
        </>
      }
    >
      <div className="stack-y-4">
        <div>
          <label
            htmlFor={modelIdInputId}
            className="block text-sm font-medium text-slate-700"
          >
            Model ID
          </label>
          <Input
            id={modelIdInputId}
            value={modelId}
            onChange={(e) => setModelId(e.target.value)}
            placeholder="manual/my-model-v1"
          />
        </div>
        <div>
          <label
            htmlFor={displayNameInputId}
            className="block text-sm font-medium text-slate-700"
          >
            Display name (optional)
          </label>
          <Input
            id={displayNameInputId}
            value={displayName}
            onChange={(e) => setDisplayName(e.target.value)}
          />
        </div>
      </div>
    </Modal>
  );
}
