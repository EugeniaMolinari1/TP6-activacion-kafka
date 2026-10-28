export const wait = (ms: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));
// Three failures total, each followed by the specified 1s / 2s / 4s backoff.
export async function retryThree<T>(
  fn: () => Promise<T>,
  sleep = wait,
): Promise<T> {
  let error: unknown;
  for (const delay of [1000, 2000, 4000]) {
    try {
      return await fn();
    } catch (e) {
      error = e;
      await sleep(delay);
    }
  }
  throw error;
}
