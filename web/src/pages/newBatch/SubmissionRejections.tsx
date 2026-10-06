import type { SubmissionRejection } from "./submissionRejection";

export default function SubmissionRejections({ rejection }: { rejection: SubmissionRejection }): JSX.Element {
  return (
    <div role="alert" className="rounded-xl border border-red-200 bg-red-50 px-5 py-4 text-sm">
      <p className="font-semibold text-red-800">
        {rejection.taskIds.length} task{rejection.taskIds.length === 1 ? "" : "s"} cannot run with this
        execution selection
      </p>
      <table className="mt-2 w-full text-left text-xs text-red-800">
        <thead>
          <tr>
            <th className="py-1 pr-4 font-medium">Task</th>
            <th className="py-1 font-medium">Reasons</th>
          </tr>
        </thead>
        <tbody>
          {rejection.taskIds.map((taskId) => (
            <tr key={taskId} className="border-t border-red-100 align-top">
              <td className="py-1 pr-4 font-mono">{taskId}</td>
              <td className="py-1 font-mono">
                {(rejection.reasons[taskId] ?? ["not runnable on this backend"]).join(", ")}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
