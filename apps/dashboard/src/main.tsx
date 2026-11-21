import { StrictMode, useCallback, useEffect, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import './style.css';

type Counts = Record<string, number>;
type Meta = { observedAt: string; freshnessMs: number; partial: boolean; warnings: string[] };
type Queue = { name: string; counts: Counts; paused?: boolean; oldestWaitingAt?: number | null };
type Worker = {
  id: string;
  queue: string;
  hostname: string;
  concurrency: number;
  version?: string;
  lastHeartbeat: number;
  heartbeatTtlMs: number;
  state: 'online' | 'stale';
  activeJobIds: string[];
  completed: number;
  failed: number;
  throughput: { completed: number; failed: number; windowMs: number; source: 'event-derived' };
  stalledSignals: number;
};
type Distribution = {
  sampleSize: number;
  p50Ms: number | null;
  p95Ms: number | null;
  p99Ms: number | null;
};
type Diagnostics = {
  windowMs: number;
  source: 'sampled';
  completed: number;
  failed: number;
  completedPerMinute: number;
  failedPerMinute: number;
  retried: number;
  errorCategories: Record<string, number>;
  wait: Distribution;
  processing: Distribution;
  trend: {
    observedAt: string;
    waiting: number;
    delayed: number;
    gapBeforeMs?: number;
    source: 'sampled';
  }[];
  outcomeTrend: {
    startsAt: string;
    completed: number;
    failed: number;
    retried: number;
    source: 'sampled';
  }[];
  limitations: string[];
};
type Flow = {
  parent: { queue: string; id: string } | null;
  children: { queue: string; id: string }[];
  source: string;
};
type Job = {
  id: string;
  name: string;
  state: string;
  attemptsMade: number;
  timestamp: number;
  processedOn?: number;
  finishedOn?: number;
  progress?: number | string;
};
type Detail = Job & {
  payload: { visibility: string; reason?: string; value?: unknown };
  returnValue: { visibility: string; reason?: string; value?: unknown };
  failureReason: string;
  stacktrace: string[];
  stalledSignals?: number;
  parentJobId?: string;
  queueWaitMs?: number;
  processingMs?: number;
  repeatJobKey?: string;
  timeline: { state: string; at: number; source: 'reconstructed' }[];
};
type Envelope<T> =
  | { success: true; data: T; meta: Meta }
  | { success: false; data: null; meta: Meta; error: { code: string; message: string } };
const states = [
  'failed',
  'waiting',
  'active',
  'delayed',
  'completed',
  'paused',
  'waiting-children',
];
const apiBase = window.location.pathname.replace(/\/$/, '');

function App() {
  const [token, setToken] = useState('');
  const [connected, setConnected] = useState(false);
  const [draft, setDraft] = useState('');
  const [queues, setQueues] = useState<Queue[]>([]);
  const [workers, setWorkers] = useState<Worker[]>([]);
  const [diagnostics, setDiagnostics] = useState<Diagnostics>();
  const [view, setView] = useState<'queues' | 'workers'>('queues');
  const [streaming, setStreaming] = useState(false);
  const lastLiveRefresh = useRef(0);
  const [theme, setTheme] = useState<'dark' | 'light'>(() =>
    window.localStorage.getItem('queuelens-theme') === 'light' ? 'light' : 'dark',
  );
  const [meta, setMeta] = useState<Meta>();
  const [jobMeta, setJobMeta] = useState<Meta>();
  const [selected, setSelected] = useState('');
  const [state, setState] = useState('failed');
  const [name, setName] = useState('');
  const [attemptsMin, setAttemptsMin] = useState(0);
  const [errorCategory, setErrorCategory] = useState('');
  const [offset, setOffset] = useState(0);
  const [jobs, setJobs] = useState<Job[]>([]);
  const [nextOffset, setNextOffset] = useState<number | null>(null);
  const [detail, setDetail] = useState<Detail>();
  const [flow, setFlow] = useState<Flow>();
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [refreshKey, setRefreshKey] = useState(0);

  const request = useCallback(
    async <T,>(path: string): Promise<Extract<Envelope<T>, { success: true }>> => {
      const response = await fetch(`${apiBase}/api/v1${path}`, {
        headers: token ? { Authorization: `Bearer ${token}` } : {},
        cache: 'no-store',
      });
      const body = (await response.json()) as Envelope<T>;
      if (!body.success) throw new Error(body.error.message);
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      return body;
    },
    [token],
  );

  useEffect(() => {
    let live = true;
    fetch(`${apiBase}/api/v1/queues`, { cache: 'no-store' })
      .then((response) => {
        if (live && response.ok) setConnected(true);
      })
      .catch(() => undefined);
    return () => {
      live = false;
    };
  }, []);

  useEffect(() => {
    if (!connected) return;
    let live = true;
    setLoading(true);
    request<Queue[]>('/queues')
      .then((body) => {
        if (!live) return;
        setQueues(body.data);
        setMeta(body.meta);
        setError('');
        setSelected((previous) =>
          body.data.some((queue) => queue.name === previous)
            ? previous
            : (body.data[0]?.name ?? ''),
        );
      })
      .catch((cause: Error) => {
        if (live) setError(cause.message);
      })
      .finally(() => {
        if (live) setLoading(false);
      });
    return () => {
      live = false;
    };
  }, [connected, request, refreshKey]);

  useEffect(() => {
    if (!connected) return;
    let live = true;
    request<{ workers: Worker[] }>('/workers')
      .then((body) => {
        if (live) setWorkers(body.data.workers);
      })
      .catch(() => {
        if (live) setWorkers([]);
      });
    return () => {
      live = false;
    };
  }, [connected, request, refreshKey]);

  useEffect(() => {
    if (!connected || !selected) return;
    let live = true;
    request<Diagnostics>(`/queues/${encodeURIComponent(selected)}/diagnostics`)
      .then((body) => {
        if (live) setDiagnostics(body.data);
      })
      .catch(() => {
        if (live) setDiagnostics(undefined);
      });
    return () => {
      live = false;
    };
  }, [connected, selected, request, refreshKey]);

  useEffect(() => {
    if (!connected) return;
    const controller = new AbortController();
    const run = async () => {
      while (!controller.signal.aborted) {
        try {
          const response = await fetch(`${apiBase}/api/v1/events`, {
            headers: token ? { Authorization: `Bearer ${token}` } : {},
            cache: 'no-store',
            signal: controller.signal,
          });
          if (!response.ok || !response.body) throw new Error('Live stream unavailable');
          setStreaming(true);
          const reader = response.body.getReader();
          const decoder = new TextDecoder();
          let buffer = '';
          while (!controller.signal.aborted) {
            const { value, done } = await reader.read();
            if (done) break;
            buffer += decoder.decode(value, { stream: true });
            const messages = buffer.split('\n\n');
            buffer = messages.pop() ?? '';
            for (const message of messages) {
              const data = message.split('\n').find((line) => line.startsWith('data: '));
              if (!data) continue;
              const body = JSON.parse(data.slice(6)) as Envelope<Queue[]>;
              if (body.success) {
                setQueues(body.data);
                setMeta(body.meta);
                if (Date.now() - lastLiveRefresh.current >= 15_000) {
                  lastLiveRefresh.current = Date.now();
                  setRefreshKey((key) => key + 1);
                }
              }
            }
          }
        } catch {
          // Polling resumes when the stream is unavailable.
        } finally {
          if (!controller.signal.aborted) setStreaming(false);
        }
        if (controller.signal.aborted) break;
        await new Promise<void>((resolve) => {
          const done = () => {
            window.clearTimeout(timer);
            controller.signal.removeEventListener('abort', done);
            resolve();
          };
          const timer = window.setTimeout(done, 3_000);
          controller.signal.addEventListener('abort', done, { once: true });
        });
      }
    };
    void run();
    return () => {
      controller.abort();
      setStreaming(false);
    };
  }, [connected, token]);

  useEffect(() => {
    if (!connected || !selected) return;
    let live = true;
    setLoading(true);
    const params = new URLSearchParams({ state, limit: '25', offset: String(offset) });
    if (name.trim()) params.set('name', name.trim());
    if (attemptsMin) params.set('attemptsMin', String(attemptsMin));
    if (errorCategory && state === 'failed') params.set('errorCategory', errorCategory);
    request<{ jobs: Job[]; nextOffset: number | null }>(
      `/queues/${encodeURIComponent(selected)}/jobs?${params}`,
    )
      .then((body) => {
        if (live) {
          setJobs(body.data.jobs);
          setNextOffset(body.data.nextOffset);
          setJobMeta(body.meta);
          setError('');
        }
      })
      .catch((cause: Error) => {
        if (live) {
          setJobs([]);
          setJobMeta(undefined);
          setError(cause.message);
        }
      })
      .finally(() => {
        if (live) setLoading(false);
      });
    return () => {
      live = false;
    };
  }, [connected, selected, state, name, attemptsMin, errorCategory, offset, request, refreshKey]);

  useEffect(() => {
    if (!connected) return;
    if (streaming) return;
    const timer = window.setInterval(() => setRefreshKey((value) => value + 1), 10_000);
    return () => window.clearInterval(timer);
  }, [connected, streaming]);

  useEffect(() => {
    if (!detail) return;
    const previous = document.activeElement as HTMLElement | null;
    const drawer = document.querySelector<HTMLElement>('[role="dialog"]');
    drawer?.querySelector<HTMLElement>('button')?.focus();
    const onKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        setDetail(undefined);
        return;
      }
      if (event.key !== 'Tab' || !drawer) return;
      const controls = Array.from(
        drawer.querySelectorAll<HTMLElement>('button, a[href], input, select'),
      );
      const first = controls[0];
      const last = controls.at(-1);
      if (!first || !last) return;
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    };
    window.addEventListener('keydown', onKey);
    return () => {
      window.removeEventListener('keydown', onKey);
      if (previous?.isConnected) previous.focus();
    };
  }, [detail]);

  const inspect = async (job: Pick<Job, 'id'>, queueName = selected) => {
    try {
      const body = await request<Detail>(
        `/queues/${encodeURIComponent(queueName)}/jobs/${encodeURIComponent(job.id)}`,
      );
      setDetail(body.data);
      setSelected(queueName);
      setView('queues');
      setFlow(undefined);
      try {
        const related = await request<Flow>(
          `/queues/${encodeURIComponent(queueName)}/jobs/${encodeURIComponent(job.id)}/flow`,
        );
        setFlow(related.data);
      } catch {
        setFlow(undefined);
      }
      setError('');
    } catch (cause) {
      setError((cause as Error).message);
    }
  };

  const select = (queue: string) => {
    setView('queues');
    setSelected(queue);
    setOffset(0);
    setDetail(undefined);
    setFlow(undefined);
    setJobMeta(undefined);
  };
  const changeState = (value: string) => {
    setState(value);
    setOffset(0);
    setDetail(undefined);
    setJobMeta(undefined);
  };
  const when = (value?: number) => (value ? new Date(value).toLocaleString() : '—');

  return (
    <div className={`app ${theme}`}>
      <header className="topbar">
        <div className="brand">
          <span className="mark">Q</span>
          <div>
            <strong>QueueLens</strong>
            <small>Read-only BullMQ monitor</small>
          </div>
        </div>
        <div className="top-actions">
          <button
            className="theme-toggle"
            onClick={() =>
              setTheme((current) => {
                const next = current === 'dark' ? 'light' : 'dark';
                window.localStorage.setItem('queuelens-theme', next);
                return next;
              })
            }
            aria-label={`Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`}
          >
            {theme === 'dark' ? '☀ Light' : '☾ Dark'}
          </button>
          <span className="readonly">● READ ONLY</span>
          {connected && (
            <button
              className="top-disconnect"
              onClick={() => {
                setToken('');
                setConnected(false);
                setQueues([]);
                setDetail(undefined);
              }}
            >
              Disconnect
            </button>
          )}
        </div>
      </header>
      {!connected ? (
        <main className="login">
          <div className="login-card">
            <div className="eyebrow">WELCOME TO QUEUELENS</div>
            <h1>See what your queues are doing.</h1>
            <p>Enter the monitor access token to inspect queue activity and failed jobs.</p>
            <form
              onSubmit={(event) => {
                event.preventDefault();
                setToken(draft);
                setConnected(true);
                setDraft('');
              }}
            >
              <label htmlFor="token">Access token</label>
              <input
                id="token"
                type="password"
                value={draft}
                onChange={(event) => setDraft(event.target.value)}
                autoComplete="off"
                required
              />
              <button type="submit">Open monitor →</button>
            </form>
            <p className="footnote">The token stays in this tab's memory.</p>
          </div>
        </main>
      ) : (
        <div className="workspace">
          <aside className="sidebar">
            <nav aria-label="Views" className="view-nav">
              <button
                className={view === 'queues' ? 'queue active' : 'queue'}
                aria-current={view === 'queues' ? 'page' : undefined}
                onClick={() => setView('queues')}
              >
                ▤ Queues
              </button>
              <button
                className={view === 'workers' ? 'queue active' : 'queue'}
                aria-current={view === 'workers' ? 'page' : undefined}
                onClick={() => setView('workers')}
              >
                ◉ Workers <span className="queue-fail">{workers.length}</span>
              </button>
            </nav>
            <div className="sidebar-label">
              QUEUES <span>{queues.length}</span>
            </div>
            <nav aria-label="Queues">
              {queues.map((queue) => (
                <button
                  key={queue.name}
                  className={
                    view === 'queues' && selected === queue.name ? 'queue active' : 'queue'
                  }
                  aria-current={view === 'queues' && selected === queue.name ? 'page' : undefined}
                  onClick={() => select(queue.name)}
                >
                  <span className="queue-icon">▤</span>
                  <span>{queue.name}</span>
                  <span className="queue-fail">{queue.counts.failed ?? 0}</span>
                </button>
              ))}
            </nav>
            <div className="sidebar-bottom">
              <button
                className="subtle"
                onClick={() => {
                  setToken('');
                  setConnected(false);
                  setQueues([]);
                  setDetail(undefined);
                }}
              >
                Disconnect
              </button>
            </div>
          </aside>
          <main className="content">
            {view === 'workers' ? (
              <>
                <div className="page-heading">
                  <div>
                    <div className="eyebrow">OPERATIONS / WORKERS</div>
                    <h1>Worker health</h1>
                    <p>
                      Only opted-in workers have a measured heartbeat. All others remain unknown.
                    </p>
                  </div>
                </div>
                <div className="meta">
                  Instrumented heartbeats have an explicit TTL. A stale heartbeat is a signal to
                  investigate, not proof of process death.
                </div>
                <section className="panel">
                  <div className="panel-head">
                    <div>
                      <h2>Instrumented workers</h2>
                      <p>
                        Active job IDs and counters come from worker events; no job payloads are
                        stored.
                      </p>
                    </div>
                  </div>
                  <div className="table-wrap">
                    <table>
                      <thead>
                        <tr>
                          <th>Worker</th>
                          <th>Queue</th>
                          <th>State</th>
                          <th>Heartbeat</th>
                          <th>Active jobs</th>
                          <th>Completed / Failed</th>
                          <th>Throughput</th>
                          <th>Stalled signals</th>
                        </tr>
                      </thead>
                      <tbody>
                        {workers.map((worker) => (
                          <tr key={`${worker.queue}:${worker.id}`}>
                            <td>
                              <strong>{worker.id}</strong>
                              <small>
                                {worker.hostname} · concurrency {worker.concurrency} ·{' '}
                                {worker.version ?? 'version unknown'}
                              </small>
                            </td>
                            <td>{worker.queue}</td>
                            <td>
                              <span className={`status ${worker.state}`}>{worker.state}</span>
                            </td>
                            <td>
                              {when(worker.lastHeartbeat)}
                              <small>TTL {Math.round(worker.heartbeatTtlMs / 1000)}s</small>
                            </td>
                            <td>
                              {worker.activeJobIds.length}
                              <small>{worker.activeJobIds.join(', ') || 'None reported'}</small>
                            </td>
                            <td>
                              {worker.completed} / {worker.failed}
                            </td>
                            <td>
                              {worker.throughput.completed} / {worker.throughput.failed}
                              <small>last minute · event-derived</small>
                            </td>
                            <td>{worker.stalledSignals}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                    {workers.length === 0 && (
                      <div className="empty">
                        No instrumented workers are visible. Uninstrumented worker liveness is
                        unknown.
                      </div>
                    )}
                  </div>
                </section>
              </>
            ) : (
              <>
                <div className="page-heading">
                  <div>
                    <div className="eyebrow">OVERVIEW / {selected || 'QUEUES'}</div>
                    <h1>{selected || 'Queues'}</h1>
                    <p>Live state counts from the configured BullMQ queue.</p>
                  </div>
                  <button className="refresh" onClick={() => setRefreshKey((value) => value + 1)}>
                    ↻ Refresh
                  </button>
                </div>
                {error && (
                  <div role="alert" className="alert">
                    {error}
                  </div>
                )}
                {meta?.partial && (
                  <div role="status" className="alert">
                    Partial or stale data. {meta.warnings.join(' ')}
                  </div>
                )}
                {jobMeta?.partial && (
                  <div role="status" className="alert">
                    {jobMeta.warnings.join(' ')}
                  </div>
                )}
                <div className="meta">
                  {loading
                    ? 'Refreshing…'
                    : meta
                      ? `Observed ${when(Date.parse(meta.observedAt))} · ${Math.round(meta.freshnessMs / 1000)}s old`
                      : 'Waiting for data'}{' '}
                  · {streaming ? 'Live stream connected' : 'Polling fallback'} · Worker liveness
                  unknown without instrumentation
                </div>
                {!selected && !loading ? (
                  <div className="empty">No queues are available to this token.</div>
                ) : (
                  <>
                    <section className="cards" aria-label="Queue counts">
                      {['waiting', 'active', 'delayed', 'completed', 'failed'].map((key) => (
                        <div className={`card ${key}`} key={key}>
                          <div className="card-label">{key}</div>
                          <div className="card-value">
                            {queues.find((queue) => queue.name === selected)?.counts[key] ?? '—'}
                          </div>
                        </div>
                      ))}
                    </section>
                    <section className="panel" aria-label="Queue comparison">
                      <div className="panel-head">
                        <div>
                          <h2>Queue comparison</h2>
                          <p>
                            Current BullMQ counts; waiting age is measured from the oldest waiting
                            job's creation time.
                          </p>
                        </div>
                      </div>
                      <div className="table-wrap">
                        <table>
                          <thead>
                            <tr>
                              <th>Queue</th>
                              <th>Backlog</th>
                              <th>Active</th>
                              <th>Failed</th>
                              <th>Oldest waiting job age</th>
                              <th>Pause state</th>
                            </tr>
                          </thead>
                          <tbody>
                            {queues.map((queue) => (
                              <tr key={queue.name}>
                                <td>
                                  <button className="link" onClick={() => select(queue.name)}>
                                    {queue.name}
                                  </button>
                                </td>
                                <td>
                                  {(queue.counts.waiting ?? 0) +
                                    (queue.counts.delayed ?? 0) +
                                    (queue.counts['waiting-children'] ?? 0)}
                                </td>
                                <td>{queue.counts.active ?? 0}</td>
                                <td>{queue.counts.failed ?? 0}</td>
                                <td>
                                  {queue.oldestWaitingAt
                                    ? `${Math.max(0, Math.round((Date.now() - queue.oldestWaitingAt) / 1000))}s`
                                    : '—'}
                                </td>
                                <td>{queue.paused ? 'Paused' : 'Running'}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </section>
                    {diagnostics && (
                      <section className="panel" aria-label="Sampled diagnostics">
                        <div className="panel-head">
                          <div>
                            <h2>Sampled diagnostics</h2>
                            <p>
                              Newest retained jobs in a five-minute window · up to 100 per completed
                              and failed state.
                            </p>
                          </div>
                          <span className="badge">SAMPLED</span>
                        </div>
                        <div className="diagnostic-grid">
                          <div>
                            <strong>{diagnostics.completedPerMinute.toFixed(1)}/min</strong>
                            <small>Completions · n={diagnostics.completed}</small>
                          </div>
                          <div>
                            <strong>{diagnostics.failedPerMinute.toFixed(1)}/min</strong>
                            <small>Failures · n={diagnostics.failed}</small>
                          </div>
                          <div>
                            <strong>
                              {diagnostics.processing.p50Ms === null
                                ? '—'
                                : `${diagnostics.processing.p50Ms} ms`}
                            </strong>
                            <small>Processing p50 · n={diagnostics.processing.sampleSize}</small>
                          </div>
                          <div>
                            <strong>
                              {diagnostics.processing.p95Ms === null
                                ? '—'
                                : `${diagnostics.processing.p95Ms} ms`}
                            </strong>
                            <small>Processing p95 · requires 20 samples</small>
                          </div>
                          <div>
                            <strong>{diagnostics.retried}</strong>
                            <small>Jobs with retries in sample</small>
                          </div>
                          <div>
                            <strong>
                              {diagnostics.wait.p50Ms === null
                                ? '—'
                                : `${diagnostics.wait.p50Ms} ms`}
                            </strong>
                            <small>Queue wait p50 · n={diagnostics.wait.sampleSize}</small>
                          </div>
                          <div>
                            <strong>
                              {diagnostics.wait.p95Ms === null
                                ? '—'
                                : `${diagnostics.wait.p95Ms} ms`}
                            </strong>
                            <small>Queue wait p95 · requires 20 samples</small>
                          </div>
                          <div>
                            <strong>
                              {diagnostics.processing.p99Ms === null
                                ? '—'
                                : `${diagnostics.processing.p99Ms} ms`}
                            </strong>
                            <small>Processing p99 · requires 100 samples</small>
                          </div>
                        </div>
                        <div className="trend" aria-label="Sampled backlog trend">
                          {diagnostics.trend.map((point) => (
                            <span
                              key={point.observedAt}
                              className={point.gapBeforeMs ? 'gap' : undefined}
                              role="img"
                              aria-label={`${point.observedAt}: ${point.waiting + point.delayed} waiting or delayed${point.gapBeforeMs ? ', collection gap before this point' : ''}`}
                              title={`${point.observedAt}: ${point.waiting + point.delayed} waiting or delayed${point.gapBeforeMs ? `; ${Math.round(point.gapBeforeMs / 1000)}s collection gap before this point` : ''}`}
                              style={{
                                height: `${Math.max(8, Math.min(100, (point.waiting + point.delayed) * 8))}%`,
                              }}
                            />
                          ))}
                          {diagnostics.trend.length === 0 && (
                            <span className="hidden">Waiting for backlog samples</span>
                          )}
                        </div>
                        <div className="outcome-trend">
                          <strong>Recent outcomes · sampled</strong>
                          <div className="table-wrap">
                            <table>
                              <thead>
                                <tr>
                                  <th>Minute starting</th>
                                  <th>Completed</th>
                                  <th>Failed</th>
                                  <th>Retried jobs</th>
                                </tr>
                              </thead>
                              <tbody>
                                {diagnostics.outcomeTrend.map((point) => (
                                  <tr key={point.startsAt}>
                                    <td>{when(Date.parse(point.startsAt))}</td>
                                    <td>{point.completed}</td>
                                    <td>{point.failed}</td>
                                    <td>{point.retried}</td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        </div>
                        <p className="diagnostic-note">
                          Error categories in sample:{' '}
                          {Object.entries(diagnostics.errorCategories)
                            .map(([key, count]) => `${key} ${count}`)
                            .join(', ') || 'none'}
                        </p>
                        <p className="diagnostic-note">{diagnostics.limitations.join(' ')}</p>
                      </section>
                    )}
                    <section className="panel">
                      <div className="panel-head">
                        <div>
                          <h2>Jobs</h2>
                          <p>Browse up to 100 jobs per page. Name search scans the newest 500.</p>
                        </div>
                        <span className="badge">
                          {queues.find((queue) => queue.name === selected)?.paused
                            ? 'PAUSED'
                            : 'QUEUE RUNNING'}
                        </span>
                      </div>
                      <div className="filters">
                        <label>
                          State
                          <select
                            value={state}
                            onChange={(event) => changeState(event.target.value)}
                          >
                            {states.map((value) => (
                              <option key={value}>{value}</option>
                            ))}
                          </select>
                        </label>
                        <label>
                          Exact job name
                          <input
                            value={name}
                            onChange={(event) => {
                              setName(event.target.value);
                              setOffset(0);
                            }}
                            placeholder="Filter by name"
                          />
                        </label>
                        <label>
                          Minimum attempts
                          <input
                            type="number"
                            min="0"
                            max="100"
                            value={attemptsMin}
                            onChange={(event) => {
                              setAttemptsMin(Number(event.target.value));
                              setOffset(0);
                            }}
                          />
                        </label>
                        {state === 'failed' && (
                          <label>
                            Error category
                            <select
                              value={errorCategory}
                              onChange={(event) => {
                                setErrorCategory(event.target.value);
                                setOffset(0);
                              }}
                            >
                              <option value="">All categories</option>
                              <option value="timeout">Timeout</option>
                              <option value="rate_limit">Rate limit</option>
                              <option value="validation">Validation</option>
                              <option value="other">Other</option>
                              <option value="unknown">Unknown</option>
                            </select>
                          </label>
                        )}
                      </div>
                      <div className="table-wrap">
                        <table>
                          <thead>
                            <tr>
                              <th>Job</th>
                              <th>State</th>
                              <th>Attempts</th>
                              <th>Created</th>
                              <th></th>
                            </tr>
                          </thead>
                          <tbody>
                            {jobs.map((job) => (
                              <tr key={job.id}>
                                <td>
                                  <strong>{job.name}</strong>
                                  <small>{job.id}</small>
                                </td>
                                <td>
                                  <span className={`status ${job.state}`}>{job.state}</span>
                                </td>
                                <td>{job.attemptsMade}</td>
                                <td>{when(job.timestamp)}</td>
                                <td>
                                  <button className="link" onClick={() => void inspect(job)}>
                                    Inspect →
                                  </button>
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                        {!loading && jobs.length === 0 && (
                          <div className="empty">No {state} jobs match this view.</div>
                        )}
                      </div>
                      <div className="pagination">
                        <button
                          disabled={offset === 0}
                          onClick={() => setOffset(Math.max(0, offset - 25))}
                        >
                          ← Previous
                        </button>
                        <span>Offset {offset}</span>
                        <button
                          disabled={nextOffset === null}
                          onClick={() => setOffset(nextOffset ?? offset)}
                        >
                          Next →
                        </button>
                      </div>
                    </section>
                  </>
                )}
              </>
            )}
          </main>
          {detail && (
            <div className="drawer-backdrop" onClick={() => setDetail(undefined)}>
              <aside
                className="drawer"
                aria-label="Job inspector"
                role="dialog"
                aria-modal="true"
                onClick={(event) => event.stopPropagation()}
              >
                <div className="drawer-head">
                  <div>
                    <div className="eyebrow">JOB INSPECTOR</div>
                    <h2>{detail.name}</h2>
                    <small>{detail.id}</small>
                  </div>
                  <button aria-label="Close inspector" onClick={() => setDetail(undefined)}>
                    ×
                  </button>
                </div>
                <dl>
                  <dt>State</dt>
                  <dd>{detail.state}</dd>
                  <dt>Attempts</dt>
                  <dd>{detail.attemptsMade}</dd>
                  <dt>Created</dt>
                  <dd>{when(detail.timestamp)}</dd>
                  <dt>Started</dt>
                  <dd>{when(detail.processedOn)}</dd>
                  <dt>Finished</dt>
                  <dd>{when(detail.finishedOn)}</dd>
                  <dt>Progress</dt>
                  <dd>{String(detail.progress ?? '—')}</dd>
                  <dt>Queue wait</dt>
                  <dd>{detail.queueWaitMs === undefined ? '—' : `${detail.queueWaitMs} ms`}</dd>
                  <dt>Processing</dt>
                  <dd>{detail.processingMs === undefined ? '—' : `${detail.processingMs} ms`}</dd>
                  <dt>Stalled signals</dt>
                  <dd>{detail.stalledSignals ?? 0} · a stall does not prove a worker died</dd>
                  <dt>Parent job</dt>
                  <dd>{detail.parentJobId ?? '—'}</dd>
                  <dt>Scheduler key</dt>
                  <dd>{detail.repeatJobKey ?? '—'}</dd>
                </dl>
                <h3>Observable timeline</h3>
                <ol>
                  {detail.timeline.map((event) => (
                    <li key={`${event.state}:${event.at}`}>
                      {event.state} · {when(event.at)}{' '}
                      <small>(reconstructed from timestamps)</small>
                    </li>
                  ))}
                </ol>
                {flow && (
                  <>
                    <h3>Flow links</h3>
                    <p className="hidden">
                      From BullMQ public dependency data; only authorized, configured queues are
                      linked.
                    </p>
                    {flow.parent && (
                      <button
                        className="link"
                        onClick={() => void inspect({ id: flow.parent!.id }, flow.parent!.queue)}
                      >
                        Parent: {flow.parent.queue} / {flow.parent.id}
                      </button>
                    )}
                    {flow.children.map((child) => (
                      <button
                        key={`${child.queue}:${child.id}`}
                        className="link flow-link"
                        onClick={() => void inspect({ id: child.id }, child.queue)}
                      >
                        Child: {child.queue} / {child.id}
                      </button>
                    ))}
                    {!flow.parent && flow.children.length === 0 && (
                      <p className="hidden">No visible flow links.</p>
                    )}
                  </>
                )}
                <h3>Payload</h3>
                <Value value={detail.payload} />
                <h3>Return value</h3>
                <Value value={detail.returnValue} />
                <h3>Failure</h3>
                <pre>{detail.failureReason || 'No failure detail available'}</pre>
                {detail.stacktrace.length > 0 && <pre>{detail.stacktrace.join('\n')}</pre>}
              </aside>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function Value({ value }: { value: Detail['payload'] }) {
  return value.visibility === 'hidden' ? (
    <p className="hidden">Hidden ({value.reason}) by monitor policy.</p>
  ) : (
    <>
      <p className="hidden">
        {value.visibility === 'partial'
          ? 'Only allowed fields are shown.'
          : 'Visible by monitor policy.'}
      </p>
      <pre>{JSON.stringify(value.value, null, 2)}</pre>
    </>
  );
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
