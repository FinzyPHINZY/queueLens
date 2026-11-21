import { StrictMode, useCallback, useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import './style.css';

type Counts = Record<string, number>;
type Meta = { observedAt: string; freshnessMs: number; partial: boolean; warnings: string[] };
type Queue = { name: string; counts: Counts; paused?: boolean };
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
};
type Envelope<T> = { data: T; meta: Meta; error?: { message: string } };
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
  const [meta, setMeta] = useState<Meta>();
  const [jobMeta, setJobMeta] = useState<Meta>();
  const [selected, setSelected] = useState('');
  const [state, setState] = useState('failed');
  const [name, setName] = useState('');
  const [offset, setOffset] = useState(0);
  const [jobs, setJobs] = useState<Job[]>([]);
  const [nextOffset, setNextOffset] = useState<number | null>(null);
  const [detail, setDetail] = useState<Detail>();
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [refreshKey, setRefreshKey] = useState(0);

  const request = useCallback(
    async <T,>(path: string): Promise<Envelope<T>> => {
      const response = await fetch(`${apiBase}/api/v1${path}`, {
        headers: token ? { Authorization: `Bearer ${token}` } : {},
        cache: 'no-store',
      });
      const body = (await response.json()) as Envelope<T>;
      if (!response.ok || body.error)
        throw new Error(body.error?.message ?? `HTTP ${response.status}`);
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
    if (!connected || !selected) return;
    let live = true;
    setLoading(true);
    const params = new URLSearchParams({ state, limit: '25', offset: String(offset) });
    if (name.trim()) params.set('name', name.trim());
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
  }, [connected, selected, state, name, offset, request, refreshKey]);

  useEffect(() => {
    if (!connected) return;
    const timer = window.setInterval(() => setRefreshKey((value) => value + 1), 10_000);
    return () => window.clearInterval(timer);
  }, [connected]);

  useEffect(() => {
    if (!detail) return;
    const escape = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setDetail(undefined);
    };
    window.addEventListener('keydown', escape);
    return () => window.removeEventListener('keydown', escape);
  }, [detail]);

  const inspect = async (job: Job) => {
    try {
      const body = await request<Detail>(
        `/queues/${encodeURIComponent(selected)}/jobs/${encodeURIComponent(job.id)}`,
      );
      setDetail(body.data);
      setError('');
    } catch (cause) {
      setError((cause as Error).message);
    }
  };

  const select = (queue: string) => {
    setSelected(queue);
    setOffset(0);
    setDetail(undefined);
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
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <span className="mark">Q</span>
          <div>
            <strong>QueueLens</strong>
            <small>Read-only BullMQ monitor</small>
          </div>
        </div>
        <div className="top-actions">
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
            <div className="sidebar-label">
              QUEUES <span>{queues.length}</span>
            </div>
            <nav aria-label="Queues">
              {queues.map((queue) => (
                <button
                  key={queue.name}
                  className={selected === queue.name ? 'queue active' : 'queue'}
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
              · Worker liveness unknown without instrumentation
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
                      <select value={state} onChange={(event) => changeState(event.target.value)}>
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
          </main>
          {detail && (
            <div className="drawer-backdrop" onClick={() => setDetail(undefined)}>
              <aside
                className="drawer"
                aria-label="Job inspector"
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
                </dl>
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
