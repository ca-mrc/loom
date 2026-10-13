import {
  useEffect,
  useId,
  useRef,
  useState,
  type ReactNode,
} from "react";

import { Button } from "./Button";
import ErrorState from "./ErrorState";
import { Input } from "./Input";
import { Modal } from "./Modal";

export type DestructiveActionConfirmation =
  | { type: "simple" }
  | { type: "typed"; expected: string; inputLabel: string };

export interface DestructiveActionDialogProps {
  open: boolean;
  title: string;
  target: string;
  consequence: string;
  confirmLabel: string;
  pendingLabel: string;
  confirmation: DestructiveActionConfirmation;
  pending: boolean;
  error: unknown | null;
  confirmDisabled?: boolean;
  children?: ReactNode;
  onClose: () => void;
  onConfirm: () => Promise<void>;
}

export function DestructiveActionDialog({
  open,
  title,
  target,
  consequence,
  confirmLabel,
  pendingLabel,
  confirmation,
  pending,
  error,
  confirmDisabled = false,
  children,
  onClose,
  onConfirm,
}: DestructiveActionDialogProps): JSX.Element {
  const formId = useId();
  const confirmationInputId = useId();
  const [typedValue, setTypedValue] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const submitLocked = useRef(false);
  const expected =
    confirmation.type === "typed" ? confirmation.expected : undefined;
  const effectivePending = pending || submitting;
  const typedConfirmationMatches =
    confirmation.type === "simple" || typedValue === confirmation.expected;

  useEffect(() => {
    setTypedValue("");
  }, [open, expected]);

  useEffect(() => {
    if (!pending && !submitting) submitLocked.current = false;
  }, [pending, submitting]);

  const handleClose = (): void => {
    if (submitLocked.current || effectivePending) return;
    onClose();
  };

  const handleConfirm = async (): Promise<void> => {
    if (
      submitLocked.current ||
      effectivePending ||
      confirmDisabled ||
      !typedConfirmationMatches
    ) {
      return;
    }
    submitLocked.current = true;
    setSubmitting(true);
    try {
      await onConfirm();
    } catch {
      // React Query owns and renders the settled mutation error.
    } finally {
      submitLocked.current = false;
      setSubmitting(false);
    }
  };

  return (
    <Modal
      open={open}
      onClose={handleClose}
      title={title}
      description={consequence}
      dismissible={!effectivePending}
      size="sm"
      footer={
        <>
          <Button onClick={handleClose} disabled={effectivePending}>
            Cancel
          </Button>
          <Button
            form={formId}
            type="submit"
            variant="danger"
            disabled={
              effectivePending || confirmDisabled || !typedConfirmationMatches
            }
          >
            {effectivePending ? pendingLabel : confirmLabel}
          </Button>
        </>
      }
    >
      <form
        id={formId}
        className="stack-y-4"
        onSubmit={(event) => {
          event.preventDefault();
          void handleConfirm();
        }}
      >
        <div className="stack-y-1 text-sm text-slate-700">
          <p className="font-medium text-slate-900">{target}</p>
        </div>

        {children}

        {confirmation.type === "typed" ? (
          <div>
            <label
              htmlFor={confirmationInputId}
              className="block text-sm font-medium text-slate-700"
            >
              {confirmation.inputLabel}
            </label>
            <Input
              id={confirmationInputId}
              value={typedValue}
              onChange={(event) => setTypedValue(event.target.value)}
              disabled={effectivePending}
              autoComplete="off"
            />
          </div>
        ) : null}

        {error && !effectivePending ? (
          <div role="alert">
            <ErrorState announce={false} error={error} />
          </div>
        ) : null}

        {effectivePending ? (
          <p
            role="status"
            aria-live="polite"
            className="sr-only"
          >
            {pendingLabel}
          </p>
        ) : null}
      </form>
    </Modal>
  );
}
