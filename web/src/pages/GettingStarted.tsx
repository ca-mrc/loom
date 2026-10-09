import { useContext } from "react";
import { AuthContext } from "../auth/authContextValue";
import { repositoryDocsVersion } from "../lib/repositoryDocs";
import { shellQuote } from "../lib/shellQuote";
import { Link, useSearchParams } from "react-router-dom";
import CommandSnippet from "../components/CommandSnippet";
import { RepositoryDocLinks } from "../components/RepositoryDocLinks";
import { Tabs } from "../components/Tabs";
import { HELP_TOPICS, HELP_TOPIC_IDS, isHelpTopic, type HelpTopicId } from "../lib/helpContent";
import { getFrontendConfig } from "../lib/frontendConfig";
import { currentServerOrigin } from "../lib/serverOrigin";

type Channel = "web" | "cli" | "api";
const CHANNELS = [{ value: "web", label: "Web" }, { value: "cli", label: "CLI" }, { value: "api", label: "API" }] as const;
const ACTION_CLASS = "inline-flex rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm font-medium text-accent hover:border-accent";

function Workflow({ channel }: { channel: Channel }): JSX.Element {
  const auth = useContext(AuthContext);
  const server = currentServerOrigin();
  const apiRoot = new URL(`${getFrontendConfig().apiRouteBase.replace(/\/$/, "")}/v1`, window.location.origin).href;
  return (
    <ol className="space-y-5">
      <li className="rounded-xl border border-slate-200 bg-white p-5">
        <h2 className="font-semibold text-slate-900">1. Connect to your team</h2>
        <p className="mt-2 text-sm leading-6 text-slate-600">Use your approved account, then confirm the team that will own your work. Request access from your team administrator if you cannot sign in.</p>
        {channel === "web" ? <div className="mt-3 flex flex-wrap items-center gap-2">{auth?.isAuthenticated ? <p className="text-sm font-medium text-emerald-700">Connected as {auth.me?.user.username} · {auth.me?.current_team?.name ?? "Select a team"}</p> : <Link className={ACTION_CLASS} to="/auth/login">Sign in</Link>}<Link className={ACTION_CLASS} to="/settings">Check your team</Link></div> : null}
        {channel === "cli" ? <div className="mt-3 space-y-3">
          <p className="text-sm text-slate-600">Install Git and uv using the full installation guide below, then clone Loom and install the CLI with Python 3.11.</p>
          <CommandSnippet label="Install CLI" command={`git clone https://github.com/ca-mrc/loom.git\ncd loom\ngit checkout ${shellQuote(repositoryDocsVersion().ref)}\nuv python install 3.11\nuv sync --locked --all-packages --python 3.11\nsource .venv/bin/activate`} />
          <p className="text-sm text-slate-600">Set LOOM_USERNAME and LOOM_PASSWORD in your terminal environment using your approved credentials. The command references them without embedding their values.</p>
          <CommandSnippet label="Connect CLI" command={`loom auth login --server ${shellQuote(server)} --username "$LOOM_USERNAME" --password env:LOOM_PASSWORD\nloom auth whoami`} />

        </div> : null}
        {channel === "api" ? <div className="mt-3 space-y-3">
          <p className="text-sm text-slate-600">Use a user-owned API token authorized for the intended team. Team owners and platform administrators can create tokens through Team access; Settings links to token management when your role allows it. If you lack access, ask your administrator about API access. Store the token in LOOM_TOKEN in your local environment.</p>
          <Link className={ACTION_CLASS} to="/settings">Check account access</Link>
          <CommandSnippet label="Check API access" command={`curl --fail-with-body ${shellQuote(`${apiRoot}/batches`)} \\\n  --header "Authorization: Bearer $LOOM_TOKEN"`} helperText="A successful response lists batches visible to the token's team." />

        </div> : null}
      </li>
      <li className="rounded-xl border border-slate-200 bg-white p-5">
        <h2 className="font-semibold text-slate-900">2. Choose tasks and a model</h2>
        <p className="mt-2 text-sm leading-6 text-slate-600">In New batch, choose evaluation for scored results or trajectory generation for agent trajectories. Evaluation uses native benchmark tasks with verifiers; custom task sets are available for trajectory generation. Select runnable tasks, then an available connection and compatible agent/model combination. Start with a small task selection.</p>
        {channel !== "web" ? <div className="mt-4 space-y-3">
          <h3 className="font-medium text-slate-900">Start from the terminal</h3>
          <p className="text-sm text-slate-600">For a minimal no-model evaluation, choose one native task with a reference solution from the task catalog and replace REPLACE_WITH_TASK_ID below. Oracle runs that solution and its verifier; it makes no model calls. Model-based workflows require an available provider and are covered in the full CLI/API guide.</p>
          <CommandSnippet label="Create batch.json" command={'cat > batch.json <<\'JSON\'\n' + JSON.stringify({ purpose: "evaluation", task_filter: { subset_kind: "explicit", task_ids: ["REPLACE_WITH_TASK_ID"] }, combinations: [{ agent_name: "oracle", n_per_task: 1 }], trial_config: {} }, null, 2) + '\nJSON'} />
          <p className="text-sm text-slate-600">For terminal discovery, set LOOM_TOKEN to a user-owned API token for your team and inspect the task list below; follow next_cursor when more results are available.</p>
          <CommandSnippet label="Find native tasks" command={`curl --fail-with-body ${shellQuote(`${apiRoot}/tasks?limit=20`)} --header "Authorization: Bearer $LOOM_TOKEN"`} />
        </div> : null}
        <p className="mt-2 text-sm leading-6 text-slate-600">Use an existing team connection first. If none is available, ask its owner to configure one in Providers.</p>
        <div className="mt-3 flex flex-wrap gap-2"><Link className={ACTION_CLASS} to="/batches/new">Open New batch</Link><Link className={ACTION_CLASS} to="/providers">View Providers</Link><Link className={ACTION_CLASS} to="/task-sets">Browse Task sets</Link></div>
      </li>
      <li className="rounded-xl border border-slate-200 bg-white p-5">
        <h2 className="font-semibold text-slate-900">3. Submit and follow progress</h2>
        <p className="mt-2 text-sm leading-6 text-slate-600">{channel === "web" ? "Review the task count, model combinations, and budget, then submit your batch." : "Submit the batch.json you prepared above. To use your current Web form configuration instead, choose Export CLI / API in New batch and download its validated batch.json."}</p>
        {channel !== "web" ? <div className="mt-3"><CommandSnippet label="Submit prepared request" command={channel === "cli" ? "loom eval batch create --request-json @batch.json" : `curl --fail-with-body --request POST ${shellQuote(`${apiRoot}/batches`)} --header "Authorization: Bearer $LOOM_TOKEN" --header 'Content-Type: application/json' --data-binary @batch.json`} helperText="This creates one real trial. Confirm the task and team before running it." /></div> : null}
        <p className="mt-2 text-sm leading-6 text-slate-600">Monitor shows batches, trials, and resources. Open your batch to track progress, and a trial for execution details or diagnosis.</p>
        <Link className={`${ACTION_CLASS} mt-3`} to="/monitor">Open Monitor</Link>
        {channel !== "web" ? <details className="mt-4 text-sm text-slate-700">
          <summary className="cursor-pointer font-medium">Inspect your submitted batch</summary>
          <p className="my-3 text-slate-600">Set LOOM_BATCH_ID to the batch ID returned by submission.</p>
          <CommandSnippet label="Inspect batch" command={channel === "cli"
            ? 'loom eval batch show "$LOOM_BATCH_ID"\nloom eval trial list --batch-id "$LOOM_BATCH_ID"'
            : `curl --fail-with-body ${shellQuote(`${apiRoot}/batches/`)}"$LOOM_BATCH_ID" --header "Authorization: Bearer $LOOM_TOKEN"`} />
        </details> : null}
      </li>
      <li className="rounded-xl border border-slate-200 bg-white p-5">
        <h2 className="font-semibold text-slate-900">4. Inspect, download, and reuse</h2>
        <p className="mt-2 text-sm leading-6 text-slate-600">Review rewards for evaluations, trajectories and ATIF, and recorded usage. Download available artifacts from a batch or trial. Outputs still being finalized are not ready to download.</p>
        <p className="mt-2 text-sm leading-6 text-slate-600">Use Run Library to find shared results and clone a configuration into your team with your own provider connection.</p>
        {channel !== "web" ? <details className="my-4 text-sm text-slate-700">
          <summary className="cursor-pointer font-medium">Download a trial result</summary>
          <p className="my-3 text-slate-600">Set LOOM_TRIAL_ID to a completed trial ID from the batch. This saves its finalized ATIF to atif.json.</p>
          <CommandSnippet label="Download ATIF" command={channel === "cli"
            ? 'loom eval trial download "$LOOM_TRIAL_ID" --kind atif --output atif.json'
            : `curl --fail-with-body ${shellQuote(`${apiRoot}/trials/`)}"$LOOM_TRIAL_ID"/atif --header "Authorization: Bearer $LOOM_TOKEN" --output atif.json`} />
        </details> : null}

      </li>
    </ol>
  );
}

export default function GettingStarted(): JSX.Element {
  const [params, setParams] = useSearchParams();
  const isAdmin = useContext(AuthContext)?.isAdmin === true;
  const rawTopic = params.get("topic");
  const topic: HelpTopicId = isHelpTopic(rawTopic) && (rawTopic !== "rates" || isAdmin) ? rawTopic : "quickstart";
  const rawChannel = params.get("channel");
  const channel: Channel = rawChannel === "cli" || rawChannel === "api" ? rawChannel : "web";
  const content = HELP_TOPICS[topic];
  function selectTopic(next: HelpTopicId) { setParams((previous) => { const updated = new URLSearchParams(previous); updated.set("topic", next); return updated; }); }
  return (
    <div className="space-y-6">
      <header>
        <p className="text-xs font-semibold uppercase tracking-wide text-accent">Loom guide</p>
        <h1 className="mt-2 text-3xl font-semibold tracking-tight text-slate-900">Getting started</h1>
        <p className="mt-2 max-w-2xl text-sm leading-6 text-slate-600">From your first connection to usable results. These short guides link to the repository for complete instructions.</p>
      </header>
      <div className="grid gap-6 lg:grid-cols-[220px_minmax(0,1fr)]">
        <label className="block text-sm font-medium text-slate-700 lg:hidden">Guide topic
          <select aria-label="Guide topic" value={topic} onChange={(event) => selectTopic(event.target.value as HelpTopicId)} className="mt-2 w-full rounded-lg border border-slate-200 bg-white p-3">
            {HELP_TOPIC_IDS.filter((id) => id !== "rates" || isAdmin).map((id) => <option key={id} value={id}>{HELP_TOPICS[id].title}</option>)}
          </select>
        </label>
        <nav aria-label="Guide topics" className="hidden content-start gap-1 lg:flex lg:flex-col">
          {HELP_TOPIC_IDS.filter((id) => id !== "rates" || isAdmin).map((id) => <button key={id} type="button" aria-current={topic === id ? "page" : undefined} onClick={() => selectTopic(id)} className={`rounded-lg px-3 py-2 text-left text-sm font-medium ${topic === id ? "bg-indigo-50 text-accent" : "text-slate-600 hover:bg-slate-100"}`}>{HELP_TOPICS[id].title}</button>)}
        </nav>
        <section className="min-w-0 space-y-5" aria-label={content.title}>
          {topic === "quickstart" ? <>
            <div className="rounded-xl border border-slate-200 bg-slate-50 p-4 text-sm"><span className="font-medium text-slate-900">Current environment: {getFrontendConfig().environmentLabel}</span><p className="mt-1 break-all font-mono text-xs text-slate-500">{currentServerOrigin()}</p></div>
            <Tabs items={CHANNELS} value={channel} onValueChange={(next) => setParams((previous) => { const updated = new URLSearchParams(previous); updated.set("channel", next); return updated; })} ariaLabel="Quickstart channel" tabListClassName="flex gap-1 rounded-lg bg-slate-100 p-1" tabClassName={({ selected }) => `flex-1 rounded-md px-4 py-2 text-sm font-medium ${selected ? "bg-white text-accent shadow-sm" : "text-slate-600"}`} panelClassName="mt-5" renderPanel={(value) => <Workflow channel={value} />} />
            <RepositoryDocLinks docs={topic === "quickstart" ? ["install", "cli", "api", "access", "results", "reuse", "purpose"] : content.docs} />
          </> : <div className="space-y-5 rounded-xl border border-slate-200 bg-white p-5">
            <h2 className="text-xl font-semibold text-slate-900">{content.title}</h2>
            <p className="text-sm leading-6 text-slate-600">{content.summary}</p>
            <ol className="list-decimal space-y-3 pl-5 text-sm leading-6 text-slate-700">{content.steps.map((step) => <li key={step}>{step}</li>)}</ol>
            <Link className={ACTION_CLASS} to={content.action.to}>{content.action.label}</Link>
            <RepositoryDocLinks docs={content.docs} />
          </div>}
        </section>
      </div>
    </div>
  );
}
