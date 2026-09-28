import React from "react";

import { LazyRouteLoadError } from "../lib/lazyRoute";

import {
  clearBrowserTestRecoveryFault,
  IS_BROWSER_TEST_BUILD,
} from "../lib/browserTestBuild";
import {
  clearBrowserFailureConsoleRedaction,
  prepareBrowserFailureForBoundary,
  reportBrowserFailure,
} from "../lib/errorReporting";

interface BrowserErrorFallback {
  reloadRequired: boolean;
  referenceId: string;
  retry: () => void;
}

interface BrowserErrorBoundaryProps {
  children: React.ReactNode;
  pathname: string;
  renderFallback: (fallback: BrowserErrorFallback) => React.ReactNode;
  resetKey: string;
}

interface BrowserErrorBoundaryState {
  reloadRequired: boolean;
  referenceId: string | null;
  renderAttempt: number;
  resetKey: string;
}

/**
 * Route-scoped browser boundary. Raw throwables are sanitized by the shared
 * reporting bridge and are never retained in React state.
 */
export class BrowserErrorBoundary extends React.Component<
  BrowserErrorBoundaryProps,
  BrowserErrorBoundaryState
> {
  private reportedResetKey = this.props.resetKey;
  private readonly reportedReferences = new Set<string>();

  state: BrowserErrorBoundaryState = {
    referenceId: null,
    reloadRequired: false,
    renderAttempt: 0,
    resetKey: this.props.resetKey,
  };

  static getDerivedStateFromProps(
    props: BrowserErrorBoundaryProps,
    state: BrowserErrorBoundaryState,
  ): Partial<BrowserErrorBoundaryState> | null {
    if (props.resetKey === state.resetKey) return null;
    return {
      referenceId: null,
      reloadRequired: false,
      renderAttempt: state.renderAttempt + (state.referenceId ? 1 : 0),
      resetKey: props.resetKey,
    };
  }

  static getDerivedStateFromError(
    error: unknown,
  ): Partial<BrowserErrorBoundaryState> {
    return {
      reloadRequired: error instanceof LazyRouteLoadError,
      referenceId: prepareBrowserFailureForBoundary(error),
    };
  }

  componentDidCatch(error: unknown): void {
    try {
      const referenceId = prepareBrowserFailureForBoundary(error);
      if (this.reportedResetKey !== this.state.resetKey) {
        this.reportedResetKey = this.state.resetKey;
        this.reportedReferences.clear();
      }
      if (!this.reportedReferences.has(referenceId)) {
        this.reportedReferences.add(referenceId);
        reportBrowserFailure(
          "route-render",
          referenceId,
          { pathname: this.props.pathname },
        );
      }
    } finally {
      clearBrowserFailureConsoleRedaction(error);
    }
  }

  private readonly retry = (): void => {
    if (IS_BROWSER_TEST_BUILD) {
      clearBrowserTestRecoveryFault("route-render-once");
    }
    this.setState((state) => ({
      referenceId: null,
      reloadRequired: false,
      renderAttempt: state.renderAttempt + 1,
    }));
  };

  render(): React.ReactNode {
    if (this.state.referenceId) {
      return this.props.renderFallback({
        referenceId: this.state.referenceId,
        reloadRequired: this.state.reloadRequired,
        retry: this.retry,
      });
    }

    return (
      <React.Fragment
        key={this.state.renderAttempt}
      >
        {this.props.children}
      </React.Fragment>
    );
  }
}
