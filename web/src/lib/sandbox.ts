/** Product names; persisted execution-class IDs and isolation values stay unchanged. */
export function isolationLabel(isolation: string): string {
  if (isolation === "guest") return "VM sandbox";
  if (isolation === "container") return "Container sandbox";
  if (isolation === "auto") return "Auto (from task requirements)";
  return isolation;
}

export function executionClassLabel(classId?: string): string {
  const labels = new Map<string, string>(Object.entries({
    "linux-amd64-cpu-pod-v1": "Container sandbox",
    "linux-amd64-cpu-web-pod-v1": "Container sandbox",
    "linux-amd64-cpu-guest-v1": "VM sandbox",
    "linux-amd64-cpu-guest-web-v1": "VM sandbox",
    "linux-amd64-cpu-guest-auth-v1": "VM sandbox · Emulated authentication",
    "linux-amd64-cpu-guest-auth-web-v1": "VM sandbox · Emulated authentication",
  }));
  return classId ? (labels.get(classId) ?? "Other sandbox") : "Sandbox type unavailable";
}
