import { useEffect, useMemo, useRef, useState } from "react";

import {
  pipelineArtifactFileUrl,
  type PipelineArtifactDetail,
} from "../../api";
import ErrorState from "../ErrorState";
import LoadingState from "../LoadingState";
import { useBoundedJson } from "./useBoundedJson";

const JSON_LIMIT = 16 * 1024 * 1024;
const EVENT_WINDOW = 100;
const VIDEO_ROLES = [
  "rgb_composite",
  "rgb_head",
  "rgb_left_wrist",
  "rgb_right_wrist",
] as const;

type VideoRole = (typeof VIDEO_ROLES)[number];
type Descriptor = {
  role: string;
  relative_path: string;
  sha256: string;
  size_bytes: number;
  media_type: string;
};
type RolloutDocument = {
  schema_version: "behavior_rollout_bundle.v1";
  payload: {
    task_name: string;
    demo_stem: string;
    domain_outcome: string;
    success: boolean;
    step_count: number;
    recording_fps: 30;
    required_file_descriptors: Descriptor[];
    optional_audit_files: Descriptor[];
  };
};
type EventRow = Record<string, unknown> & { step_idx: number; kind: string; sourceOrder: number };

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseRollout(value: unknown): RolloutDocument | null {
  if (!isRecord(value) || value.schema_version !== "behavior_rollout_bundle.v1") return null;
  const payload = value.payload;
  if (
    !isRecord(payload) ||
    typeof payload.task_name !== "string" ||
    typeof payload.demo_stem !== "string" ||
    typeof payload.domain_outcome !== "string" ||
    typeof payload.success !== "boolean" ||
    !Number.isSafeInteger(payload.step_count) ||
    Number(payload.step_count) <= 0 ||
    payload.recording_fps !== 30 ||
    !Array.isArray(payload.required_file_descriptors) ||
    !Array.isArray(payload.optional_audit_files)
  ) return null;
  const descriptors = payload.required_file_descriptors;
  if (
    descriptors.length !== 7 ||
    !descriptors.every(
      (item) =>
        isRecord(item) &&
        typeof item.role === "string" &&
        typeof item.relative_path === "string" &&
        typeof item.sha256 === "string" &&
        typeof item.size_bytes === "number" &&
        typeof item.media_type === "string" &&
        item.size_bytes > 0 &&
        /^sha256:[0-9a-f]{64}$/u.test(item.sha256),
    )
  ) return null;
  if (descriptors.map((item) => item.role).join(",") !== [
    "rollout_hdf5",
    "bddl_transitions",
    "scene_metadata",
    "rgb_head",
    "rgb_left_wrist",
    "rgb_right_wrist",
    "rgb_composite",
  ].join(",")) return null;
  return value as unknown as RolloutDocument;
}

function eventsFrom(value: unknown): EventRow[] | null {
  if (!isRecord(value) || !Array.isArray(value.transitions) || !Array.isArray(value.grasp_history)) {
    return null;
  }
  const rows: EventRow[] = [];
  for (const [kind, values] of [
    ["transition", value.transitions],
    ["grasp", value.grasp_history],
  ] as const) {
    for (const item of values) {
      if (!isRecord(item) || !Number.isSafeInteger(item.step_idx) || Number(item.step_idx) < 0) {
        return null;
      }
      rows.push({ ...item, step_idx: Number(item.step_idx), kind, sourceOrder: rows.length });
    }
  }
  return rows.sort(
    (left, right) => left.step_idx - right.step_idx || left.sourceOrder - right.sourceOrder,
  );
}

function sceneFrom(value: unknown): {
  robot: string;
  objects: Array<{ ordinal: number; scene_name: string; joint_position_count: number }>;
  identities: Array<{ scope_name: string; scene_name: string }>;
} | null {
  if (
    !isRecord(value) ||
    value.schema_version !== "behavior.rollout-scene-projection.v1" ||
    typeof value.robot_scene_name !== "string" ||
    !Array.isArray(value.state_objects) ||
    !Array.isArray(value.inst_to_name)
  ) return null;
  if (
    !value.state_objects.every(
      (item) =>
        isRecord(item) &&
        Number.isSafeInteger(item.ordinal) &&
        typeof item.scene_name === "string" &&
        Number.isSafeInteger(item.joint_position_count),
    ) ||
    !value.inst_to_name.every(
      (item) =>
        isRecord(item) &&
        typeof item.scope_name === "string" &&
        typeof item.scene_name === "string",
    )
  ) return null;
  return {
    robot: value.robot_scene_name,
    objects: value.state_objects as Array<{ ordinal: number; scene_name: string; joint_position_count: number }>,
    identities: value.inst_to_name as Array<{ scope_name: string; scene_name: string }>,
  };
}

function fileUrl(artifact: PipelineArtifactDetail, descriptor: Descriptor | undefined): string | null {
  if (!descriptor) return null;
  const file = artifact.files.find(
    (candidate) =>
      candidate.relative_path === descriptor.relative_path &&
      candidate.media_type === descriptor.media_type &&
      candidate.size_bytes === descriptor.size_bytes &&
      candidate.sha256 === descriptor.sha256,
  );
  return file ? pipelineArtifactFileUrl(artifact.id, file.file_index) : null;
}

export default function BehaviorRolloutViewer({
  artifact,
}: {
  artifact: PipelineArtifactDetail;
}): JSX.Element {
  const semantic = artifact.files.find((file) => file.role === "semantic_document");
  const semanticState = useBoundedJson(
    semantic ? pipelineArtifactFileUrl(artifact.id, semantic.file_index) : null,
    JSON_LIMIT,
  );
  const document = semanticState.status === "ready" ? parseRollout(semanticState.value) : null;
  const descriptorByRole = useMemo(
    () => new Map((document?.payload.required_file_descriptors ?? []).map((item) => [item.role, item])),
    [document],
  );
  const transition = descriptorByRole.get("bddl_transitions");
  const sceneDescriptor = descriptorByRole.get("scene_metadata");
  const transitionUrl = transition && transition.size_bytes <= JSON_LIMIT
    ? fileUrl(artifact, transition)
    : null;
  const sceneUrl = fileUrl(artifact, sceneDescriptor);
  const eventState = useBoundedJson(transitionUrl, JSON_LIMIT);
  const sceneState = useBoundedJson(sceneUrl, JSON_LIMIT);
  const events = eventState.status === "ready" ? eventsFrom(eventState.value) : null;
  const scene = sceneState.status === "ready" ? sceneFrom(sceneState.value) : null;
  const videos = useRef<Partial<Record<VideoRole, HTMLVideoElement | null>>>({});
  const [search, setSearch] = useState("");
  const [windowStart, setWindowStart] = useState(0);

  const filtered = useMemo(() => {
    const query = search.normalize("NFC").trim().toLocaleLowerCase();
    if (!events) return [];
    return query
      ? events.filter((event) => JSON.stringify(event).toLocaleLowerCase().includes(query))
      : events;
  }, [events, search]);
  useEffect(() => setWindowStart(0), [search]);

  const syncFollowers = (force = false): void => {
    const master = videos.current.rgb_composite;
    if (!master) return;
    for (const role of VIDEO_ROLES.slice(1)) {
      const follower = videos.current[role];
      if (!follower) continue;
      if (force || Math.abs(follower.currentTime - master.currentTime) > 0.1) {
        follower.currentTime = master.currentTime;
      }
      follower.playbackRate = master.playbackRate;
    }
  };
  const playFollowers = (): void => {
    syncFollowers(true);
    for (const role of VIDEO_ROLES.slice(1)) void videos.current[role]?.play().catch(() => undefined);
  };
  const pauseFollowers = (): void => {
    for (const role of VIDEO_ROLES.slice(1)) videos.current[role]?.pause();
  };
  const seek = (step: number): void => {
    const seconds = step / 30;
    for (const role of VIDEO_ROLES) {
      const video = videos.current[role];
      if (video) video.currentTime = seconds;
    }
  };

  if (!semantic) return <ErrorState error={new Error("Artifact has no semantic document")} />;
  if (semanticState.status === "idle" || semanticState.status === "loading") return <LoadingState />;
  if (semanticState.status === "error") return <ErrorState error={semanticState.error} />;
  if (!document) return <ErrorState error={new Error("Rollout semantic contract is invalid")} />;
  const videoEntries = VIDEO_ROLES.map((role) => ({
    role,
    url: fileUrl(artifact, descriptorByRole.get(role)),
  }));
  if (videoEntries.some(({ url }) => url === null)) {
    return <ErrorState error={new Error("Rollout video inventory does not match its signed descriptors")} />;
  }
  const hdf5 = descriptorByRole.get("rollout_hdf5");
  const hdf5Url = fileUrl(artifact, hdf5);
  const eventDownloadUrl = fileUrl(artifact, transition);

  return <div className="stack-y-6">
    <section aria-labelledby="rollout-summary" className="rounded border p-4">
      <h2 id="rollout-summary" className="text-lg font-semibold">Behavior rollout</h2>
      <p>{document.payload.task_name} · {document.payload.demo_stem} · {document.payload.domain_outcome}</p>
      <p>{document.payload.step_count} steps at {document.payload.recording_fps} fps</p>
    </section>
    <section aria-labelledby="rollout-videos">
      <h2 id="rollout-videos" className="text-lg font-semibold">Synchronized cameras</h2>
      <div className="grid gap-4 md:grid-cols-2">
        {videoEntries.map(({ role, url }) => <figure key={role}>
          <figcaption className="font-medium">{role.replaceAll("_", " ")}</figcaption>
          <video
            ref={(node) => { videos.current[role] = node; }}
            aria-label={role.replaceAll("_", " ")}
            className="w-full rounded bg-black"
            controls
            preload="metadata"
            src={url!}
            onTimeUpdate={role === "rgb_composite" ? () => syncFollowers() : undefined}
            onSeeked={role === "rgb_composite" ? () => syncFollowers(true) : undefined}
            onRateChange={role === "rgb_composite" ? () => syncFollowers() : undefined}
            onPlay={role === "rgb_composite" ? playFollowers : undefined}
            onPause={role === "rgb_composite" ? pauseFollowers : undefined}
          />
        </figure>)}
      </div>
    </section>
    <section aria-labelledby="rollout-events" className="stack-y-3">
      <h2 id="rollout-events" className="text-lg font-semibold">Events</h2>
      {transition && transition.size_bytes > JSON_LIMIT ? <p>
        Event JSON is larger than 16 MiB and is not parsed in the browser.{" "}
        {eventDownloadUrl ? <a href={eventDownloadUrl} className="text-accent">Download events</a> : null}
      </p> : eventState.status === "loading" || eventState.status === "idle" ? <LoadingState /> : eventState.status === "error" ? <ErrorState error={eventState.error} /> : !events ? <ErrorState error={new Error("Event JSON contract is invalid")} /> : <>
        <label className="block">Search events<input aria-label="Search events" value={search} onChange={(event) => setSearch(event.target.value)} className="ml-2 rounded border px-2 py-1" /></label>
        <p>{filtered.length} matching events</p>
        <ul aria-label="Virtualized rollout events" className="max-h-96 overflow-auto rounded border">
          {filtered.slice(windowStart, windowStart + EVENT_WINDOW).map((event) => <li key={event.sourceOrder}>
            <button
              type="button"
              className="block w-full border-b p-2 text-left font-mono text-xs hover:bg-slate-50"
              onClick={() => seek(event.step_idx)}
            >step {event.step_idx} · {event.kind} · {JSON.stringify(event)}</button>
          </li>)}
        </ul>
        <div className="flex gap-2">
          <button type="button" disabled={windowStart === 0} onClick={() => setWindowStart(Math.max(0, windowStart - EVENT_WINDOW))}>Previous events</button>
          <button type="button" disabled={windowStart + EVENT_WINDOW >= filtered.length} onClick={() => setWindowStart(windowStart + EVENT_WINDOW)}>Next events</button>
        </div>
      </>}
    </section>
    <section aria-labelledby="rollout-scene" className="stack-y-3">
      <h2 id="rollout-scene" className="text-lg font-semibold">Scene projection</h2>
      {sceneState.status === "loading" || sceneState.status === "idle" ? <LoadingState /> : sceneState.status === "error" ? <ErrorState error={sceneState.error} /> : !scene ? <ErrorState error={new Error("Scene projection contract is invalid")} /> : <>
        <p>Robot: <strong>{scene.robot}</strong></p>
        <table className="w-full"><caption>Scene objects</caption><thead><tr><th scope="col">Ordinal</th><th scope="col">Name</th><th scope="col">Joint positions</th></tr></thead><tbody>{scene.objects.map((item) => <tr key={item.ordinal}><td>{item.ordinal}</td><td>{item.scene_name}</td><td>{item.joint_position_count}</td></tr>)}</tbody></table>
        <table className="w-full"><caption>Instance identities</caption><thead><tr><th scope="col">Scope</th><th scope="col">Scene name</th></tr></thead><tbody>{scene.identities.map((item) => <tr key={item.scope_name}><td>{item.scope_name}</td><td>{item.scene_name}</td></tr>)}</tbody></table>
      </>}
    </section>
    <section aria-labelledby="rollout-downloads">
      <h2 id="rollout-downloads" className="text-lg font-semibold">Downloads</h2>
      {hdf5Url ? <a href={hdf5Url} className="text-accent">Download rollout HDF5</a> : <p>HDF5 unavailable</p>}
    </section>
  </div>;
}
