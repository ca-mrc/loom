import { useState, type ReactNode } from "react";

import { Button } from "./Button";
import CommandSnippet from "./CommandSnippet";
import { Modal } from "./Modal";

interface CommandActionsProps {
  title: string;
  label?: string;
  commands?: string[];
  children?: ReactNode;
  description?: string;
}

/** Resource-specific commands stay out of the operational page until requested. */
export function CommandActions({
  title,
  label = "View CLI commands",
  commands,
  children,
  description = "Use a CLI session connected to this Loom server and the same team as the web app.",
}: CommandActionsProps): JSX.Element {
  const [open, setOpen] = useState(false);
  return (
    <>
      <Button variant="secondary" size="sm" onClick={() => setOpen(true)}>
        {label}
      </Button>
      <Modal open={open} onClose={() => setOpen(false)} title={title} description={description} size="lg">
        <div className="stack-y-4">
          {commands?.map((command, index) => (
            <CommandSnippet key={`${index}:${command}`} label={`${title} ${index + 1}`} command={command} />
          ))}
          {children}
        </div>
      </Modal>
    </>
  );
}
