import { useState } from "react";
import { Link } from "react-router-dom";

import { useAuth } from "../auth/useAuth";
import { Button } from "../components/Button";
import { completeManagedLogin, hasManagedLoginProof } from "../lib/managedLogin";

export default function ManagedLogin(): JSX.Element {
  const { loginComplete } = useAuth();
  const [state, setState] = useState<"ready" | "pending" | "done" | "failed">(
    () => hasManagedLoginProof() ? "ready" : "failed",
  );

  async function signIn(): Promise<void> {
    if (state !== "ready") return;
    setState("pending");
    try {
      await completeManagedLogin(loginComplete);
      setState("done");
    } catch {
      setState("failed");
    }
  }

  return (
    <div className="mx-auto my-16 max-w-lg space-y-4 px-4">
      <h1 className="text-2xl font-semibold">Personal environment login</h1>
      <p>This signs you into this environment only. Your management login stays separate.</p>
      {state === "ready" && (
        <Button onClick={() => { void signIn(); }}>Sign into this environment</Button>
      )}
      {state === "pending" && <p role="status">Signing in…</p>}
      {state === "done" && <>
        <p role="status">Signed in to this environment.</p>
        <Link to="/settings">Open environment settings</Link>
      </>}
      {state === "failed" && <p role="alert">
        Sign-in unavailable. Request a fresh browser login with <code>loom dev login ENVIRONMENT_ID --browser</code>.
      </p>}
    </div>
  );
}
