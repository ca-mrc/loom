/** Advanced form defaults and trial-config validation. */

import type { RetryReason } from "./advancedInputs";

export { RETRY_REASONS, clampFloat, clampInt, type RetryReason } from "./advancedInputs";

export interface AdvancedState {
  forceBuild: boolean;
  deleteEnv: boolean;
  verifierEnvMode: "" | "shared" | "separate";
  isolation: "" | "container" | "guest";
  networkPolicy: "" | "gateway-only" | "web-allowlist" | "public-web";
  allowedWebsites: string;
  skipVerifier: boolean;
  overrideAgentTimeoutSec: string;
  agentTimeoutMultiplier: string;
  overrideVerifierTimeoutSec: string;
  verifierTimeoutMultiplier: string;
  overrideEnvBuildTimeoutSec: string;
  envBuildTimeoutMultiplier: string;
  maxAttempts: string;
  retryOn: Set<RetryReason>;
  backoffBaseSec: string;
  backoffMaxSec: string;
  backoffMultiplier: string;
  backoffJitter: string;
  submitPriority: string;
  multiModelEnabled: boolean;
  teacherModelName: string;
  teacherEpisodes: string;
  multiModelPolicy: "student_teacher_student" | "beta_mixture" | "student_to_teacher_turns";
  multiModelBeta: string;
  multiModelSeed: string;
  multiModelStepStart: string;
  multiModelStepEnd: string;
}

export const INITIAL_ADVANCED: AdvancedState = {
  forceBuild: false,
  deleteEnv: true,
  verifierEnvMode: "",
  isolation: "",
  networkPolicy: "",
  allowedWebsites: "",
  skipVerifier: false,
  overrideAgentTimeoutSec: "",
  agentTimeoutMultiplier: "1",
  overrideVerifierTimeoutSec: "",
  verifierTimeoutMultiplier: "1",
  overrideEnvBuildTimeoutSec: "",
  envBuildTimeoutMultiplier: "1",
  maxAttempts: "1",
  retryOn: new Set<RetryReason>(),
  backoffBaseSec: "30",
  backoffMaxSec: "600",
  backoffMultiplier: "2",
  backoffJitter: "0.2",
  submitPriority: "100",
  multiModelEnabled: false,
  teacherModelName: "",
  teacherEpisodes: "2",
  multiModelPolicy: "student_teacher_student",
  multiModelBeta: "0.6",
  multiModelSeed: "",
  multiModelStepStart: "2",
  multiModelStepEnd: "9",
};

export function buildNetworkPolicyOverride(
  s: Pick<AdvancedState, "networkPolicy" | "allowedWebsites">,
): { ok: true; value: Record<string, unknown> | null } | { ok: false; error: string } {
  const allowWeb = s.allowedWebsites.split(/\r?\n/).map((value) => value.trim()).filter(Boolean);
  if (!s.networkPolicy && allowWeb.length > 0) {
    return { ok: false, error: "Approved websites require the Approved websites network policy." };
  }
  if (s.networkPolicy && s.networkPolicy !== "web-allowlist" && allowWeb.length > 0) {
    return { ok: false, error: "Approved websites can only be set for the Approved websites policy." };
  }
  if (s.networkPolicy === "web-allowlist") {
    if (allowWeb.length === 0) return { ok: false, error: "Add at least one approved website." };
    const destinations: Array<{ host: string; protocol: "http" | "https" }> = [];
    for (const raw of allowWeb) {
      let url: URL;
      try {
        url = new URL(raw);
      } catch {
        return { ok: false, error: `${raw} is not a valid website URL.` };
      }
      if (
        !["http:", "https:"].includes(url.protocol) || url.username || url.password || url.port ||
        (url.pathname !== "" && url.pathname !== "/") || url.search || url.hash
      ) {
        return { ok: false, error: `${raw} must be an exact http(s)://hostname without a port or path.` };
      }
      destinations.push({ host: url.hostname, protocol: url.protocol.slice(0, -1) as "http" | "https" });
    }
    const unique = Array.from(
      new Map(destinations.map((item) => [`${item.host}\0${item.protocol}`, item])).values(),
    ).sort((a, b) => a.host.localeCompare(b.host) || a.protocol.localeCompare(b.protocol));
    return { ok: true, value: { kind: "web-allowlist", destinations: unique } };
  }
  return { ok: true, value: s.networkPolicy ? { kind: s.networkPolicy } : null };
}

export function buildAdvancedConfig(
  s: AdvancedState,
): { ok: true; value: Record<string, unknown> } | { ok: false; error: string } {
  const out: Record<string, unknown> = {};
  if (s.forceBuild) out.force_build = true;
  if (!s.deleteEnv) out.delete_env = false;
  if (s.skipVerifier) out.skip_verifier = true;
  if (s.verifierEnvMode) out.verifier_env_mode = s.verifierEnvMode;
  if (s.isolation) out.isolation = s.isolation;
  const networkPolicy = buildNetworkPolicyOverride(s);
  if (!networkPolicy.ok) return networkPolicy;
  if (networkPolicy.value !== null) out.baseline_network_policy_override = networkPolicy.value;
  const numOrErr = (
    raw: string,
    name: string,
    opts: { min: number; max?: number; allowEmpty?: boolean } = { min: 0 },
  ): number | undefined | string => {
    if (raw === "") {
      return opts.allowEmpty ? undefined : `${name} is required.`;
    }
    const n = Number.parseFloat(raw);
    if (!Number.isFinite(n)) return `${name} must be a number.`;
    if (n < opts.min) return `${name} must be ≥ ${opts.min}.`;
    if (opts.max !== undefined && n > opts.max) {
      return `${name} must be ≤ ${opts.max}.`;
    }
    return n;
  };
  const opt = (raw: string, name: string, min: number) => {
    if (raw === "") return undefined;
    return numOrErr(raw, name, { min });
  };

  const overrideAgent = opt(s.overrideAgentTimeoutSec, "Override agent timeout", 0.001);
  if (typeof overrideAgent === "string") return { ok: false, error: overrideAgent };
  if (overrideAgent !== undefined) out.override_agent_timeout_sec = overrideAgent;
  const agentMult = numOrErr(s.agentTimeoutMultiplier, "Agent timeout multiplier", { min: 0.001 });
  if (typeof agentMult === "string") return { ok: false, error: agentMult };
  if (agentMult !== 1) out.agent_timeout_multiplier = agentMult;

  const overrideVer = opt(s.overrideVerifierTimeoutSec, "Override verifier timeout", 0.001);
  if (typeof overrideVer === "string") return { ok: false, error: overrideVer };
  if (overrideVer !== undefined) out.override_verifier_timeout_sec = overrideVer;
  const verMult = numOrErr(s.verifierTimeoutMultiplier, "Verifier timeout multiplier", { min: 0.001 });
  if (typeof verMult === "string") return { ok: false, error: verMult };
  if (verMult !== 1) out.verifier_timeout_multiplier = verMult;

  const overrideBuild = opt(s.overrideEnvBuildTimeoutSec, "Override env-build timeout", 0.001);
  if (typeof overrideBuild === "string") return { ok: false, error: overrideBuild };
  if (overrideBuild !== undefined) out.override_env_build_timeout_sec = overrideBuild;
  const buildMult = numOrErr(s.envBuildTimeoutMultiplier, "Env-build timeout multiplier", { min: 0.001 });
  if (typeof buildMult === "string") return { ok: false, error: buildMult };
  if (buildMult !== 1) out.env_build_timeout_multiplier = buildMult;

  const maxAttempts = numOrErr(s.maxAttempts, "Max attempts", { min: 1, max: 20 });
  if (typeof maxAttempts === "string") return { ok: false, error: maxAttempts };
  const wantsRetry = (maxAttempts as number) > 1 && s.retryOn.size > 0;
  if (wantsRetry) {
    const base = numOrErr(s.backoffBaseSec, "Backoff base seconds", { min: 0.001 });
    if (typeof base === "string") return { ok: false, error: base };
    const max = numOrErr(s.backoffMaxSec, "Backoff max seconds", { min: 0.001 });
    if (typeof max === "string") return { ok: false, error: max };
    const mult = numOrErr(s.backoffMultiplier, "Backoff multiplier", { min: 0.001 });
    if (typeof mult === "string") return { ok: false, error: mult };
    const jitter = numOrErr(s.backoffJitter, "Backoff jitter", { min: 0, max: 1 });
    if (typeof jitter === "string") return { ok: false, error: jitter };
    if ((max as number) < (base as number)) {
      return {
        ok: false,
        error: "Backoff max seconds must be ≥ backoff base seconds.",
      };
    }
    out.retry = {
      max_attempts: maxAttempts,
      retry_on: Array.from(s.retryOn),
      backoff: {
        base_sec: base,
        max_sec: max,
        multiplier: mult,
        jitter: jitter,
      },
    };
  } else {
    out.retry = { max_attempts: 1, retry_on: [] };
  }

  const prio = numOrErr(s.submitPriority, "Submit priority", { min: 0, max: 1000 });
  if (typeof prio === "string") return { ok: false, error: prio };
  if (prio !== 100) out.submit_priority = prio;

  if (s.multiModelEnabled) {
    const teacher = s.teacherModelName.trim();
    if (!teacher) {
      return { ok: false, error: "Teacher model name is required when multi-model is enabled." };
    }
    if (s.multiModelPolicy === "beta_mixture") {
      const beta = Number.parseFloat(s.multiModelBeta);
      if (!Number.isFinite(beta) || beta < 0 || beta > 1) {
        return { ok: false, error: "Beta must be a number in [0, 1]." };
      }
      out.multi_model = {
        enabled: true,
        policy: "beta_mixture",
        beta,
        secondary_model: {
          provider: "openai",
          name: teacher,
          source: "api",
        },
      };
      const mixSeed = s.multiModelSeed.trim();
      if (mixSeed) {
        (out.multi_model as Record<string, unknown>).mix_seed = mixSeed;
      }
    } else if (s.multiModelPolicy === "student_to_teacher_turns") {
      const stepStart = numOrErr(s.multiModelStepStart, "Step start", { min: 2 });
      if (typeof stepStart === "string") return { ok: false, error: stepStart };
      const stepEnd = numOrErr(s.multiModelStepEnd, "Step end", { min: 3 });
      if (typeof stepEnd === "string") return { ok: false, error: stepEnd };
      if (stepEnd === undefined || stepStart === undefined || stepEnd <= stepStart) {
        return { ok: false, error: "Step end must be greater than step start." };
      }
      out.multi_model = {
        enabled: true,
        policy: "student_to_teacher_turns",
        step_start: stepStart,
        step_end: stepEnd,
        secondary_model: {
          provider: "openai",
          name: teacher,
          source: "api",
        },
      };
      const mixSeed = s.multiModelSeed.trim();
      if (mixSeed) {
        (out.multi_model as Record<string, unknown>).mix_seed = mixSeed;
      }
    } else {
      const episodes = numOrErr(s.teacherEpisodes, "Teacher episodes", { min: 1, max: 1000 });
      if (typeof episodes === "string") return { ok: false, error: episodes };
      out.multi_model = {
        enabled: true,
        policy: "student_teacher_student",
        teacher_episodes: episodes,
        secondary_model: {
          provider: "openai",
          name: teacher,
          source: "api",
        },
      };
    }
  }

  return { ok: true, value: out };
}
