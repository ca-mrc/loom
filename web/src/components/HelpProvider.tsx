import { useContext, useState, type ReactNode } from "react";
import { type HelpTopicId } from "../lib/helpContent";
import { AuthContext } from "../auth/authContextValue";
import { HelpContext } from "./helpContext";
import { lazyRoute } from "../lib/lazyRoute";
import { RouteRecoveryBoundary } from "./RouteRecoveryBoundary";
const HelpDialog = lazyRoute(() => import("./HelpDialog"));

export function HelpProvider({ children }: { children: ReactNode }): JSX.Element {
  const [topic, setTopic] = useState<HelpTopicId | null>(null);
  const isAdmin = useContext(AuthContext)?.isAdmin === true;
  const visibleTopic = topic === "rates" && !isAdmin ? "quickstart" : topic;
  return (
    <HelpContext.Provider value={setTopic}>
      {children}
      {visibleTopic && <RouteRecoveryBoundary><HelpDialog topic={visibleTopic} onClose={() => setTopic(null)} /></RouteRecoveryBoundary>}
    </HelpContext.Provider>
  );
}
