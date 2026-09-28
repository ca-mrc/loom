import { Suspense } from "react";
import { lazyRoute } from "../lib/lazyRoute";
import LoadingState from "./LoadingState";

const JsonTree = lazyRoute(() => import("./JsonTree"));

export interface JsonViewerProps {
  data: unknown;
  /** Expand all nodes; otherwise expand only the first two levels. */
  expanded?: boolean;
  className?: string;
}

export default function JsonViewer(props: JsonViewerProps): JSX.Element {
  return <Suspense fallback={<LoadingState label="Loading JSON details…" />}><JsonTree {...props} /></Suspense>;
}
