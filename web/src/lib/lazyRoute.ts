import { lazy, type ComponentType, type LazyExoticComponent } from "react";

/** Only failed module imports require reload; page render errors remain retryable. */
export class LazyRouteLoadError extends Error {
  constructor() {
    super("This page could not be loaded.");
  }
}

export function lazyRoute<P extends object>(
  load: () => Promise<{ default: ComponentType<P> }>,
): LazyExoticComponent<ComponentType<P>> {
  return lazy(() => load().catch(() => {
    // Do not retain the rejected URL, browser message, or arbitrary throwable.
    throw new LazyRouteLoadError();
  }));
}
