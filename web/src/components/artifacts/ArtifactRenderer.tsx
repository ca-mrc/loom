import { Suspense, type ComponentType } from "react";

import type { PipelineArtifactDetail } from "../../api";
import LoadingState from "../LoadingState";
import { lazyRoute } from "../../lib/lazyRoute";
import GenericArtifactViewer from "./GenericArtifactViewer";

const BehaviorRolloutViewer = lazyRoute(() => import("./BehaviorRolloutViewer"));

export type ArtifactViewerProps = { artifact: PipelineArtifactDetail };

export const ARTIFACT_RENDERERS: Readonly<
  Record<string, ComponentType<ArtifactViewerProps>>
> = Object.freeze({
  "behavior_rollout_bundle.v1": BehaviorRolloutViewer,
});

export default function ArtifactRenderer(props: ArtifactViewerProps): JSX.Element {
  const Renderer = ARTIFACT_RENDERERS[props.artifact.artifact_type] ?? GenericArtifactViewer;
  return <Suspense fallback={<LoadingState label="Loading artifact viewer…" />}><Renderer {...props} /></Suspense>;
}
