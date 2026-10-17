// Cancellation limits obsolete work; the generation check also protects against
// responses that complete even when their transport ignores cancellation.
export function createValidationRequests() {
  let generation = 0;
  let controller = null;

  const cancel = () => {
    generation++;
    controller?.abort();
    controller = null;
  };

  return {
    cancel,
    start() {
      cancel();
      const currentGeneration = generation;
      const currentController = new AbortController();
      controller = currentController;
      return {
        signal: currentController.signal,
        isCurrent: () => generation === currentGeneration && !currentController.signal.aborted,
        finish() {
          if (generation === currentGeneration) controller = null;
        },
      };
    },
  };
}
