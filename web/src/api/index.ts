/** Canonical domain API composition. Only core owns transport state. */
import { adminApi } from "./admin";
import { authApi } from "./auth";
import { catalogApi } from "./catalog";
import { libraryApi } from "./library";
import { overviewApi } from "./overview";
import { pipelineApi } from "./pipeline";
import { providersApi } from "./providers";
import { runsApi } from "./runs";
import { usageApi } from "./usage";

export const api = {
  ...pipelineApi,
  ...overviewApi,
  ...runsApi,
  ...authApi,
  ...catalogApi,
  ...providersApi,
  ...adminApi,
  ...libraryApi,
  ...usageApi,
};
export {
  type AccountActionApproval,
  type AdminAuditEvent,
  type ApiTokenEntry,
  type ApiTokenList,
  type ApiTokenReveal,
  type InviteCreateBody,
  type InviteEntry,
  type InviteLookup,
  type InviteReveal,
  type InviteRole,
  type InviteStatus,
  type PasswordResetRequestEntry,
  type TeamRegistrationApproval,
  type TeamRegistrationApprovalBody,
  type TeamRegistrationEntry,
  type TeamRegistrationRequestBody,
  type UserRegistrationEntry,
} from "./admin";
export { loadAuthSession, parseAuthMe, type AuthMe, type AuthTeam, type PublicTeam } from "./auth";
export {
  type BenchmarkTagsResponse,
  type TaskList,
  type TaskRow,
  type TaskSetDetailResponse,
  type TaskSetListItem,
  type TaskSetListResponse,
  type TaskSetSubmitResponse,
  type TaskSetWarning,
} from "./catalog";
export {
  apiDownload,
  apiFetch,
  apiUpload,
  AuthSessionLoadError,
  setCsrfToken,
  setUnauthorizedHandler,
  type ApiError,
  type AuthSessionLoadFailureKind,
} from "./core";
export {
  type ArtifactGroup,
  type ArtifactInventory,
  type ArtifactSummary,
  type CloneRunLibraryBatchResult,
  type RetryDefaultSnapshotMismatch,
  type RetryPolicyView,
  type ReuseRunLibraryArtifactResult,
  type RunLibraryArtifact,
  type RunLibraryBatch,
  type RunLibraryBatchDetail,
  type RunLibraryBatchList,
  type RunLibraryOwnerTeam,
  type RunVisibility,
  type ShareStatus,
} from "./library";
export {
  type OverviewAction,
  type OverviewActionKind,
  type OverviewStatus,
  type OverviewSummary,
} from "./overview";
export {
  cancelPipelineRun,
  getPipelineArtifact,
  getPipelineLivePreviewFrame,
  getPipelineLivePreviewMetadata,
  getPipelineRun,
  getPipelineStageRun,
  listPipelineArtifacts,
  listPipelineEvents,
  listPipelineRuns,
  listPipelineStageAttempts,
  listPipelineStageRuns,
  pipelineArtifactDownloadUrl,
  pipelineArtifactFileUrl,
  retryPipelineStageRun,
  type PipelineArtifactDetail,
  type PipelineArtifactListResponse,
  type PipelineArtifactSummary,
  type PipelineCancelResponse,
  type PipelineEventPage,
  type PipelineExecutionAttemptList,
  type PipelineLivePreviewFrame,
  type PipelineLivePreviewMetadata,
  type PipelineNodeTopology,
  type PipelineRetryBody,
  type PipelineRetryResponse,
  type PipelineRunDetail,
  type PipelineRunListItem,
  type PipelineRunListParams,
  type PipelineRunListResponse,
  type PipelineStageRunDetail,
  type PipelineStageRunListResponse,
  type PipelineStageRunSummary,
} from "./pipeline";
export {
  type AgentVersionEntry,
  type Backend,
  type ModelEntry,
  OPENAI_SHAPED_CONNECTION_TYPES,
  type ProviderConnectionCreateBody,
  type ProviderConnectionDetail,
  type ProviderConnectionEntry,
  type ProviderConnectionModelEntry,
  type ProviderConnectionModelsRefreshResult,
  type ProviderConnectionPatchBody,
  type ProviderConnectionTestResult,
} from "./providers";
export {
  type AdminTeam,
  type Combination,
  type CombinationSummary,
  type CreateBatchBody,
  type DebugEvidence,
  type DeliveryExport,
  type DiagnosisReport,
  type MonitorSummary,
  type NetworkPolicyPreview,
  type RerunPlan,
  type TaskFilter,
  type TrialDetail,
} from "./runs";
