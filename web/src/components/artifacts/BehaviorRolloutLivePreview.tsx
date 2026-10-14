import { useEffect, useRef, useState } from "react";
import { Link, useNavigate } from "react-router-dom";

import {
  api,
  type PipelineExecutionAttemptList,
  type PipelineLivePreviewMetadata,
} from "../../api";

const MIN_POLL_MS = 500;
const MIN_STALLED_MS = 5_000;
type Attempt = PipelineExecutionAttemptList["items"][number];
type Frame = { attemptId: string; dataUrl: string; etag: string; generation: string; sequence: number };

function isAbort(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError";
}

function elapsedLabel(startedAt: string | null, now: number): string {
  if (!startedAt) return "not started";
  const seconds = Math.max(0, Math.floor((now - Date.parse(startedAt)) / 1_000));
  const minutes = Math.floor(seconds / 60);
  return minutes > 0 ? `${minutes}m ${seconds % 60}s` : `${seconds}s`;
}

function ageLabel(receivedAt: string | null, now: number): string {
  if (!receivedAt) return "waiting for first frame";
  return `${Math.max(0, Math.floor((now - Date.parse(receivedAt)) / 1_000))}s ago`;
}

export default function BehaviorRolloutLivePreview({
  attempt,
  committedArtifactPath,
  onHandoff,
  runId,
  stageRunId,
}: {
  attempt: Attempt;
  committedArtifactPath: string | null;
  onHandoff: () => void;
  runId: string;
  stageRunId: string;
}): JSX.Element | null {
  const navigate = useNavigate();
  const [metadata, setMetadata] = useState<PipelineLivePreviewMetadata | null>(null);
  const [frame, setFrame] = useState<Frame | null>(null);
  const [failedAttempt, setFailedAttempt] = useState<string | null>(null);
  const [now, setNow] = useState(() => Date.now());
  const metadataController = useRef<AbortController | null>(null);
  const frameController = useRef<AbortController | null>(null);
  const generation = useRef<string | null>(null);
  const latestSequence = useRef<number | null>(null);
  const etag = useRef<string | null>(null);
  const hasMetadata = useRef(false);
  const onHandoffRef = useRef(onHandoff);

  useEffect(() => { onHandoffRef.current = onHandoff; }, [onHandoff]);

  useEffect(() => {
    if (committedArtifactPath) navigate(committedArtifactPath, { replace: true });
  }, [committedArtifactPath, navigate]);

  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 1_000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    if (attempt.state !== "running" || committedArtifactPath) return;
    hasMetadata.current = false;
    generation.current = null;
    latestSequence.current = null;
    etag.current = null;
    let disposed = false;
    let timer: number | null = null;
    let epoch = 0;

    const abort = (): void => {
      epoch += 1;
      metadataController.current?.abort();
      frameController.current?.abort();
      metadataController.current = null;
      frameController.current = null;
      if (timer !== null) window.clearTimeout(timer);
      timer = null;
    };
    const schedule = (delay: number): void => {
      if (!disposed && document.visibilityState === "visible") {
        timer = window.setTimeout(() => void poll(), delay);
      }
    };
    const poll = async (): Promise<void> => {
      if (disposed || document.visibilityState !== "visible") return;
      const requestEpoch = epoch;
      const controller = new AbortController();
      metadataController.current = controller;
      try {
        const next = await api.getPipelineLivePreviewMetadata(
          runId,
          stageRunId,
          attempt.id,
          controller.signal,
        );
        if (disposed || requestEpoch !== epoch || next.attempt_id !== attempt.id) return;
        metadataController.current = null;
        hasMetadata.current = true;
        setFailedAttempt(null);
        setMetadata(next);
        if (next.generation !== generation.current) {
          generation.current = next.generation;
          latestSequence.current = null;
          etag.current = null;
          setFrame(null);
        }
        if (next.state === "handoff") {
          onHandoffRef.current();
          return;
        }
        if (next.state === "ended") return;
        if (
          next.latest_sequence !== null &&
          next.latest_sequence !== latestSequence.current
        ) {
          const requestedGeneration = next.generation;
          const requestedSequence = next.latest_sequence;
          const controller = new AbortController();
          frameController.current = controller;
          const response = await api.getPipelineLivePreviewFrame(
            runId,
            stageRunId,
            attempt.id,
            requestedSequence,
            etag.current,
            controller.signal,
          );
          if (
            disposed ||
            requestEpoch !== epoch ||
            requestedGeneration !== generation.current
          ) return;
          frameController.current = null;
          if (response.status === "ready") {
            latestSequence.current = requestedSequence;
            etag.current = response.etag;
            setFrame({
              attemptId: attempt.id,
              dataUrl: response.data_url,
              etag: response.etag,
              generation: requestedGeneration,
              sequence: requestedSequence,
            });
          } else {
            latestSequence.current = requestedSequence;
            setFrame((current) => current ? { ...current, sequence: requestedSequence } : current);
          }
        }
        schedule(Math.max(MIN_POLL_MS, next.retry_after_ms));
      } catch (error) {
        if (disposed || requestEpoch !== epoch || isAbort(error)) return;
        metadataController.current = null;
        frameController.current = null;
        setFailedAttempt(hasMetadata.current ? attempt.id : null);
        schedule(MIN_POLL_MS);
      }
    };
    const onVisibilityChange = (): void => {
      abort();
      if (document.visibilityState === "visible") schedule(0);
    };
    document.addEventListener("visibilitychange", onVisibilityChange);
    schedule(0);
    return () => {
      disposed = true;
      document.removeEventListener("visibilitychange", onVisibilityChange);
      abort();
    };
  }, [attempt.id, attempt.state, committedArtifactPath, runId, stageRunId]);

  if (committedArtifactPath) {
    return <p role="status">Preview committed. Opening the immutable <Link className="text-accent" to={committedArtifactPath}>Behavior rollout Artifact</Link>.</p>;
  }
  const currentMetadata = metadata?.attempt_id === attempt.id ? metadata : null;
  const currentFrame = frame?.attemptId === attempt.id ? frame : null;
  const failed = failedAttempt === attempt.id;
  if (attempt.state !== "running" || (!currentMetadata && !failed)) return null;
  const receivedAt = currentMetadata?.received_at ?? null;
  const stalled = currentMetadata?.state === "live" && receivedAt !== null &&
    now - Date.parse(receivedAt) > Math.max(MIN_STALLED_MS, currentMetadata.retry_after_ms * 3);
  return <section aria-labelledby="behavior-live-preview-heading" className="stack-y-3 rounded border p-3">
    <div className="flex flex-wrap items-center justify-between gap-2">
      <h3 id="behavior-live-preview-heading" className="font-semibold">Behavior rollout live preview</h3>
      <strong className="rounded border-2 border-current px-2 py-1 text-xs">LIVE / UNVERIFIED</strong>
    </div>
    <div aria-live="polite" role="status" className="grid gap-1 md:grid-cols-2">
      <p><strong>Attempt:</strong> <span className="font-mono text-xs">{attempt.id}</span></p>
      <p><strong>Backend:</strong> {attempt.worker_pool_class ?? "redacted"}</p>
      <p><strong>Verified CP lifecycle:</strong> {attempt.state}</p>
      <p><strong>Preview lifecycle:</strong> {currentMetadata?.state ?? "unavailable"}</p>
      <p><strong>Latest step:</strong> {currentMetadata?.latest_step_idx ?? "—"}</p>
      <p><strong>Elapsed:</strong> {elapsedLabel(attempt.started_at, now)}</p>
      <p><strong>VLA/simulator readiness:</strong> {currentMetadata?.state === "live" ? "validated composite received" : "waiting for validated composite"}</p>
      <p><strong>Last frame:</strong> {ageLabel(receivedAt, now)}</p>
    </div>
    {failed ? <p role="alert" className="rounded border-2 border-dashed border-current p-2">⚠ Live preview is temporarily unavailable. Stage status is unchanged.</p> : null}
    {stalled ? <p role="alert" className="rounded border-2 border-dashed border-current p-2">⚠ Preview is stalled. Stage status is unchanged.</p> : null}
    {currentFrame ? <figure>
      <img
        alt="Live unverified composite: head, left wrist, right wrist, and bounded status tile"
        className="w-full rounded bg-black"
        data-generation={currentFrame.generation}
        data-sequence={currentFrame.sequence}
        src={currentFrame.dataUrl}
      />
      <figcaption>Composite frame {currentFrame.sequence}; final evidence is available only after Artifact commit.</figcaption>
    </figure> : <p>Waiting for the first validated composite frame.</p>}
  </section>;
}
