export const REQUEST_TIMEOUT_MS = 15000;

// Keep the deadline active through body consumption, not just response headers.
// The race also settles callers when a transport fails to honour AbortSignal.
export async function request(url, init, consume, { signal, timeoutMs = REQUEST_TIMEOUT_MS } = {}) {
  const controller = new AbortController();
  const cancel = () => controller.abort(new DOMException("Request cancelled.", "AbortError"));
  if (signal?.aborted) cancel();
  else signal?.addEventListener("abort", cancel, { once: true });

  let timer;
  let onAbort;
  try {
    if (controller.signal.aborted) throw controller.signal.reason;
    const cancelled = new Promise((resolve, reject) => {
      onAbort = () => reject(controller.signal.reason);
      controller.signal.addEventListener("abort", onAbort, { once: true });
      timer = setTimeout(() => controller.abort(new DOMException(
        "The request timed out. Check the connection status before retrying.", "TimeoutError"
      )), timeoutMs);
    });
    const operation = (async () => {
      const response = await fetch(url, { ...init, signal: controller.signal });
      controller.signal.throwIfAborted();
      return await consume(response);
    })();
    return await Promise.race([operation, cancelled]);
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener("abort", cancel);
    if (onAbort) controller.signal.removeEventListener("abort", onAbort);
  }
}

export const isRequestInterruption = error =>
  error?.name === "AbortError" || error?.name === "TimeoutError";
