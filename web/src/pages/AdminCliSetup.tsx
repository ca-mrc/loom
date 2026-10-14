import { HelpButton } from "../components/HelpButton";
import { currentServerOrigin } from "../lib/serverOrigin";
export function CliSetupCommands({ token }: { token: string }): JSX.Element {
  const commands = [
    `export LOOM_API_TOKEN=${token}`,
    `loom auth login --server ${currentServerOrigin()} --token env:LOOM_API_TOKEN`,
    "loom auth whoami",
  ];
  return (
    <div className="stack-y-2">
      <p className="text-sm font-medium text-emerald-950">CLI setup commands</p>
      <div className="stack-y-1 rounded-lg border border-emerald-200 bg-white p-3">
        {commands.map((command) => (
          <code
            key={command}
            className="block whitespace-pre-wrap break-words font-mono text-xs leading-relaxed text-slate-800"
          >
            {command}
          </code>
        ))}
      </div>
      <HelpButton topic="quickstart">Run your first batch</HelpButton>
    </div>
  );
}
