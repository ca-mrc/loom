import { useMutation } from "@tanstack/react-query";
import { useRef } from "react";

import { Button, type ButtonProps } from "./Button";
import ErrorState from "./ErrorState";

interface DownloadButtonProps extends Omit<ButtonProps, "onClick" | "children"> {
  actionKey: string;
  download: () => Promise<void>;
  children: string;
}

/** Each download owns its pending/error state until its target is retired. */
export function DownloadButton({ actionKey, ...props }: DownloadButtonProps): JSX.Element {
  return <DownloadAction key={actionKey} {...props} />;
}

function DownloadAction({ download, children, disabled, ...props }: Omit<DownloadButtonProps, "actionKey">): JSX.Element {
  const started = useRef(false);
  const action = useMutation({
    mutationFn: download,
    onSettled: () => { started.current = false; },
  });
  return <div className="min-w-0 stack-y-2">
    <Button
      {...props}
      disabled={disabled || action.isPending}
      aria-busy={action.isPending}
      onClick={() => {
        // Guard the interval before React renders the pending disabled state.
        if (started.current) return;
        started.current = true;
        action.mutate();
      }}
    >
      {action.isPending ? "Downloading…" : action.isError ? `Retry ${children}` : children}
    </Button>
    {action.isError ? <ErrorState error={action.error} /> : null}
  </div>;
}
