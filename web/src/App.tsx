import { Navigate, Route, Routes } from "react-router-dom";

import Layout from "./components/Layout";
import { lazyRoute } from "./lib/lazyRoute";

import { RouteRecoveryBoundary } from "./components/RouteRecoveryBoundary";
import { SkipLink } from "./components/SkipLink";

const ManagedLogin = lazyRoute(() => import("./pages/ManagedLogin"));

const AdminAccess = lazyRoute(() => import("./pages/AdminAccess"));
const Benchmarks = lazyRoute(() => import("./pages/Benchmarks"));
const ProviderCreate = lazyRoute(() => import("./pages/ProviderCreate"));
const ProviderDetail = lazyRoute(() => import("./pages/ProviderDetail"));
const ProvidersList = lazyRoute(() => import("./pages/ProvidersList"));
const BatchDetail = lazyRoute(() => import("./pages/BatchDetail"));
const Home = lazyRoute(() => import("./pages/Home"));
const GettingStarted = lazyRoute(() => import("./pages/GettingStarted"));
const InviteAccept = lazyRoute(() => import("./pages/InviteAccept"));
const Monitor = lazyRoute(() => import("./pages/Monitor"));
const PipelineRunDetail = lazyRoute(() => import("./pages/PipelineRunDetail"));
const PipelineArtifactDetail = lazyRoute(() => import("./pages/PipelineArtifactDetail"));
const PipelineRuns = lazyRoute(() => import("./pages/PipelineRuns"));
const NewBatch = lazyRoute(() => import("./pages/NewBatch"));
const NotFound = lazyRoute(() => import("./pages/NotFound"));
const PasswordAction = lazyRoute(() => import("./pages/PasswordAction"));
const AuthLogin = lazyRoute(() => import("./pages/AuthLogin"));
const RateCardsAdmin = lazyRoute(() => import("./pages/RateCardsAdmin"));
const RunLibrary = lazyRoute(() => import("./pages/RunLibrary"));
const RunLibraryBatchDetail = lazyRoute(() => import("./pages/RunLibraryBatchDetail"));
const Settings = lazyRoute(() => import("./pages/Settings"));
const TaskSetDetail = lazyRoute(() => import("./pages/TaskSetDetail"));
const TaskSetsList = lazyRoute(() => import("./pages/TaskSetsList"));
const TaskSetSubmit = lazyRoute(() => import("./pages/TaskSetSubmit"));
const Tasks = lazyRoute(() => import("./pages/Tasks"));
const TrialCompare = lazyRoute(() => import("./pages/TrialCompare"));
const TrialDetail = lazyRoute(() => import("./pages/TrialDetail"));
const UsageDashboard = lazyRoute(() => import("./pages/UsageDashboard"));

export default function App(): JSX.Element {
  return (
    <Routes>
      {/* Keep proof consumption mounted while auth state changes the app shell. */}
      <Route path="/auth/managed" element={
        <><SkipLink /><main id="main-content" tabIndex={-1}>
          <RouteRecoveryBoundary><ManagedLogin /></RouteRecoveryBoundary>
        </main></>
      } />
      <Route path="/" element={<Layout />}>
        <Route index element={<Home />} />
        <Route path="getting-started" element={<GettingStarted />} />
        <Route path="monitor" element={<Monitor />} />
        <Route path="pipelines" element={<PipelineRuns />} />
        <Route path="pipeline-artifacts/:artifactId" element={<PipelineArtifactDetail />} />
        <Route path="pipelines/:runId" element={<PipelineRunDetail />} />
        <Route path="pipelines/:runId/stages/:stageRunId/artifacts/:artifactId" element={<PipelineArtifactDetail />} />
        <Route path="library" element={<RunLibrary />} />
        <Route path="library/batches/:batchId" element={<RunLibraryBatchDetail />} />
        {/* Legacy redirects — preserve old links from external docs / chats. */}
        <Route path="trials" element={<Navigate to="/monitor?view=trials" replace />} />
        <Route path="batches" element={<Navigate to="/monitor?view=batches" replace />} />
        <Route path="trials/compare" element={<TrialCompare />} />
        <Route path="trials/:trialId" element={<TrialDetail />} />
        <Route path="batches/new" element={<NewBatch />} />
        <Route path="batches/:batchId" element={<BatchDetail />} />
        {/* Tasks / Benchmarks / Usage routes still resolve so power-user
            URLs don't 404 mid-redesign; they're absent from nav per PR-4. */}
        <Route path="tasks" element={<Tasks />} />
        <Route path="benchmarks" element={<Benchmarks />} />
        <Route path="usage" element={<UsageDashboard />} />
        <Route path="task-sets/new" element={<TaskSetSubmit />} />
        <Route path="task-sets/detail" element={<TaskSetDetail />} />
        <Route path="task-sets/:id" element={<TaskSetDetail />} />
        <Route path="task-sets" element={<TaskSetsList />} />
        <Route path="providers/new" element={<ProviderCreate />} />
        <Route path="providers/:id" element={<ProviderDetail />} />
        <Route path="providers" element={<ProvidersList />} />
        <Route path="admin/access" element={<AdminAccess />} />
        <Route path="auth/login" element={<AuthLogin />} />
        <Route path="auth/setup" element={<PasswordAction mode="setup" />} />
        <Route path="auth/reset" element={<PasswordAction mode="reset" />} />
        <Route path="invites/accept" element={<InviteAccept />} />
        <Route path="rate-cards" element={<RateCardsAdmin />} />
        <Route path="settings" element={<Settings />} />
        <Route path="*" element={<NotFound />} />
      </Route>
    </Routes>
  );
}
