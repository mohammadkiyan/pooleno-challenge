import { useCallback, useEffect, useRef, useState } from "react";
import type { LiveRun, ScenarioId } from "../shared/live.ts";

async function read<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, { ...init, signal: AbortSignal.timeout(10_000) });
  const value = await response.json();
  if (!response.ok) throw Object.assign(new Error(value.error ?? `Runner returned HTTP ${response.status}`), { status: response.status });
  return value as T;
}
export function useLiveRun() {
  const [run, setRun] = useState<LiveRun>();
  const [error, setError] = useState("");
  const [starting, setStarting] = useState(false);
  const mounted = useRef(true);
  const submitting = useRef(false);
  const generation = useRef(0);
  useEffect(() => {
    mounted.current = true;
    const initialGeneration = generation.current;
    void read<LiveRun | null>("/live/active").then(value => { if (mounted.current && value && generation.current === initialGeneration) setRun(value); }).catch(() => {});
    return () => { mounted.current = false; };
  }, []);
  const id = run?.status === "running" ? run.id : undefined;
  useEffect(() => {
    if (!id) return;
    let stopped = false;
    let timer: ReturnType<typeof setTimeout>;
    const poll = async () => {
      try {
        const value = await read<LiveRun>(`/live/runs/${id}`);
        if (stopped) return;
        setRun(value);
        setError("");
        if (value.status !== "running") return;
      } catch (err) {
        if (stopped) return;
        if (err instanceof Error && "status" in err && err.status === 404) {
          const message = "Run capture is no longer available; the runner may have restarted. Start a fresh run.";
          setRun(previous => previous ? { ...previous, status: "failed", error: message } : previous);
          setError(message);
          return;
        }
        setError(`${err instanceof Error ? err.message : String(err)}. Reconnecting to the active run…`);
      }
      timer = setTimeout(() => void poll(), 400);
    };
    void poll();
    return () => { stopped = true; clearTimeout(timer); };
  }, [id]);
  const start = useCallback(async (scenarioId: ScenarioId, variantId: string) => {
    if (submitting.current) return;
    submitting.current = true;
    generation.current += 1;
    setStarting(true); setRun(undefined); setError("");
    try {
      const value = await read<LiveRun>("/live/runs", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ scenarioId, variantId }) });
      if (mounted.current) setRun(value);
    } catch (err) {
      if (mounted.current) setError(err instanceof Error ? err.message : String(err));
      // If the start response was lost, recover ownership of the already-started run.
      try { const active = await read<LiveRun | null>("/live/active"); if (mounted.current && active?.status === "running") setRun(active); } catch {}
    } finally { submitting.current = false; if (mounted.current) setStarting(false); }
  }, []);
  return { run, error: error || run?.error || "", busy: starting || run?.status === "running", start };
}
