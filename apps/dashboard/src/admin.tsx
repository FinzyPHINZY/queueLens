import { useEffect, useState } from 'react';

type Action =
  | 'retry'
  | 'pause'
  | 'resume'
  | 'promote'
  | 'remove'
  | 'bulk-retry'
  | 'clean'
  | 'cancel-request';
type Capabilities = { enabled: boolean; queues: { name: string; actions: Action[] }[] };
type Audit = {
  operationId: string;
  actorId: string;
  action: string;
  queue: string;
  targetId?: string;
  phase: string;
  outcome?: string;
  code?: string;
  at: string;
};
type Preview = {
  jobIds: string[];
  eligible: { id: string; eligible: boolean; code?: string }[];
  previewToken: string;
  expiresAt: string;
};
type Result = {
  operationId: string;
  action: string;
  status: string;
  items?: { id: string; status: string; code?: string }[];
};

export function AdminView({ base, defaultQueue }: { base: string; defaultQueue: string }) {
  const [draftToken, setDraftToken] = useState('');
  const [adminToken, setAdminToken] = useState('');
  const [capabilities, setCapabilities] = useState<Capabilities>();
  const [audit, setAudit] = useState<Audit[]>([]);
  const [queue, setQueue] = useState(defaultQueue);
  const [action, setAction] = useState<Action>('retry');
  const [jobId, setJobId] = useState('');
  const [jobIds, setJobIds] = useState('');
  const [cleanState, setCleanState] = useState<'completed' | 'failed'>('failed');
  const [olderMinutes, setOlderMinutes] = useState(60);
  const [limit, setLimit] = useState(10);
  const [preview, setPreview] = useState<Preview>();
  const [confirm, setConfirm] = useState(false);
  const [operationKey, setOperationKey] = useState('');
  const [message, setMessage] = useState('');
  const [busy, setBusy] = useState(false);

  async function api<T>(
    path: string,
    method = 'GET',
    body?: object,
    idempotencyKey?: string,
  ): Promise<T> {
    const csrf =
      document.querySelector<HTMLMetaElement>('meta[name="queueharbor-csrf"]')?.content ??
      document.querySelector<HTMLMetaElement>('meta[name="queuelens-csrf"]')?.content;
    const response = await fetch(`${base}/api/v1/admin${path}`, {
      method,
      credentials: 'same-origin',
      cache: 'no-store',
      headers: {
        ...(adminToken ? { Authorization: `Bearer ${adminToken}` } : {}),
        ...(body ? { 'Content-Type': 'application/json' } : {}),
        ...(csrf ? { 'X-QueueHarbor-CSRF': csrf, 'X-Queuelens-CSRF': csrf } : {}),
        ...(idempotencyKey ? { 'Idempotency-Key': idempotencyKey } : {}),
      },
      body: body ? JSON.stringify(body) : undefined,
    });
    const envelope = (await response.json()) as {
      success: boolean;
      data: T;
      error?: { code: string; message: string };
    };
    if (!response.ok || !envelope.success)
      throw new Error(envelope.error?.code ?? `HTTP ${response.status}`);
    return envelope.data;
  }

  useEffect(() => {
    let live = true;
    api<Capabilities>('/capabilities')
      .then((data) => {
        if (!live) return;
        setCapabilities(data);
        setQueue((current) =>
          data.queues.some((item) => item.name === current)
            ? current
            : (data.queues[0]?.name ?? ''),
        );
        setMessage('');
      })
      .catch((cause: Error) => {
        if (!live) return;
        setCapabilities(undefined);
        setMessage(
          cause.message === 'INVALID_REQUEST'
            ? 'Management mode is disabled on this server.'
            : 'Enter an authorized admin token to manage queues.',
        );
      });
    return () => {
      live = false;
    };
  }, [adminToken, base]);

  useEffect(() => {
    if (!capabilities) return;
    let live = true;
    api<Audit[]>('/audit')
      .then((data) => {
        if (live) setAudit(data);
      })
      .catch(() => {
        if (live) setAudit([]);
      });
    return () => {
      live = false;
    };
  }, [capabilities, adminToken]);

  useEffect(() => {
    setPreview(undefined);
    setConfirm(false);
    setOperationKey('');
  }, [action, queue, jobId, jobIds, cleanState, olderMinutes, limit]);
  const permitted = capabilities?.queues.find((item) => item.name === queue)?.actions ?? [];
  const selectedAction = permitted.includes(action) ? action : permitted[0];
  const bulk = selectedAction === 'bulk-retry' || selectedAction === 'clean';
  const command = bulk
    ? selectedAction === 'bulk-retry'
      ? {
          jobIds: jobIds
            .split(',')
            .map((id) => id.trim())
            .filter(Boolean),
        }
      : { state: cleanState, olderThanMs: olderMinutes * 60_000, limit }
    : selectedAction === 'pause' || selectedAction === 'resume'
      ? {}
      : { jobId: jobId.trim() };

  async function prepare() {
    if (!selectedAction) return;
    setBusy(true);
    setMessage('');
    try {
      if (bulk) {
        const result = await api<Preview>(
          `/queues/${encodeURIComponent(queue)}/preview/${selectedAction}`,
          'POST',
          command,
        );
        setPreview(result);
      }
      setOperationKey(
        Array.from(crypto.getRandomValues(new Uint8Array(16)))
          .map((byte) => byte.toString(16).padStart(2, '0'))
          .join(''),
      );
      setConfirm(true);
    } catch (cause) {
      setMessage((cause as Error).message);
    } finally {
      setBusy(false);
    }
  }

  async function execute() {
    if (!selectedAction) return;
    setBusy(true);
    setMessage('');
    try {
      const request = bulk
        ? { ...command, jobIds: preview?.jobIds, previewToken: preview?.previewToken }
        : command;
      if (!operationKey) throw new Error('Review the action again');
      const result = await api<Result>(
        `/queues/${encodeURIComponent(queue)}/actions/${selectedAction}`,
        'POST',
        request,
        operationKey,
      );
      setMessage(`${result.status}: ${result.action} · operation ${result.operationId}`);
      if (result.items?.length)
        setMessage(
          (current) =>
            `${current} · ${result.items!.map((item) => `${item.id}: ${item.status}${item.code ? ` (${item.code})` : ''}`).join(', ')}`,
        );
      setConfirm(false);
      setPreview(undefined);
      setOperationKey('');
      setAudit(await api<Audit[]>('/audit'));
    } catch (cause) {
      setMessage((cause as Error).message);
    } finally {
      setBusy(false);
    }
  }

  return (
    <>
      <div className="page-heading">
        <div>
          <div className="eyebrow">OPERATIONS / MANAGEMENT</div>
          <h1>Queue management</h1>
          <p>
            Each action requires server enablement, an admin permission, confirmation, and an audit
            record.
          </p>
        </div>
      </div>
      <div className="meta">
        A read token cannot perform management actions. Admin credentials stay in this tab's memory.
        A queue pause does not interrupt active handlers.
      </div>
      <section className="panel">
        <div className="panel-head">
          <h2>Admin access</h2>
        </div>
        <form
          className="filters"
          onSubmit={(event) => {
            event.preventDefault();
            setAdminToken(draftToken);
          }}
        >
          <label htmlFor="admin-token">Admin token</label>
          <input
            id="admin-token"
            type="password"
            autoComplete="off"
            value={draftToken}
            onChange={(event) => setDraftToken(event.target.value)}
          />
          <button type="submit" className="refresh">
            Unlock management
          </button>
        </form>
        {message && (
          <div role="status" className="alert">
            {message}
          </div>
        )}
      </section>
      {capabilities && (
        <>
          <section className="panel">
            <div className="panel-head">
              <div>
                <h2>Action</h2>
                <p>Only actions permitted for the selected queue appear here.</p>
              </div>
            </div>
            <div className="filters">
              <label htmlFor="admin-queue">Queue</label>
              <select
                id="admin-queue"
                value={queue}
                onChange={(event) => setQueue(event.target.value)}
              >
                {capabilities.queues.map((item) => (
                  <option key={item.name}>{item.name}</option>
                ))}
              </select>
              <label htmlFor="admin-action">Action</label>
              <select
                id="admin-action"
                value={selectedAction}
                onChange={(event) => setAction(event.target.value as Action)}
              >
                {permitted.map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </select>
              {selectedAction &&
                !['pause', 'resume', 'bulk-retry', 'clean'].includes(selectedAction) && (
                  <>
                    <label htmlFor="admin-job">Job ID</label>
                    <input
                      id="admin-job"
                      value={jobId}
                      onChange={(event) => setJobId(event.target.value)}
                    />
                  </>
                )}
              {selectedAction === 'bulk-retry' && (
                <>
                  <label htmlFor="admin-ids">Failed job IDs (comma separated, max 25)</label>
                  <input
                    id="admin-ids"
                    value={jobIds}
                    onChange={(event) => setJobIds(event.target.value)}
                  />
                </>
              )}
              {selectedAction === 'clean' && (
                <>
                  <label htmlFor="clean-state">Terminal state</label>
                  <select
                    id="clean-state"
                    value={cleanState}
                    onChange={(event) =>
                      setCleanState(event.target.value as 'completed' | 'failed')
                    }
                  >
                    <option value="failed">failed</option>
                    <option value="completed">completed</option>
                  </select>
                  <label htmlFor="clean-age">Older than (minutes)</label>
                  <input
                    id="clean-age"
                    type="number"
                    min="1"
                    max="43200"
                    value={olderMinutes}
                    onChange={(event) => setOlderMinutes(Number(event.target.value))}
                  />
                  <label htmlFor="clean-limit">Maximum jobs</label>
                  <input
                    id="clean-limit"
                    type="number"
                    min="1"
                    max="25"
                    value={limit}
                    onChange={(event) => setLimit(Number(event.target.value))}
                  />
                </>
              )}
            </div>
            {selectedAction && (
              <button className="refresh" disabled={busy} onClick={() => void prepare()}>
                {bulk ? 'Preview action' : 'Review action'}
              </button>
            )}
            {preview && (
              <div className="meta">
                Preview expires {new Date(preview.expiresAt).toLocaleString()}.{' '}
                {preview.eligible
                  .map((item) => `${item.id}: ${item.eligible ? 'eligible' : item.code}`)
                  .join(' · ')}
              </div>
            )}
            {confirm && (
              <div className="alert" role="status">
                <p>
                  Confirm {selectedAction} on {queue}
                  {bulk ? ` for ${preview?.jobIds.length ?? 0} jobs` : jobId ? ` / ${jobId}` : ''}?
                </p>
                <button className="refresh" disabled={busy} onClick={() => void execute()}>
                  Confirm {selectedAction}
                </button>
                <button className="refresh" onClick={() => setConfirm(false)}>
                  Cancel
                </button>
              </div>
            )}
          </section>
          <section className="panel">
            <div className="panel-head">
              <h2>Recent audit events</h2>
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Time</th>
                    <th>Actor</th>
                    <th>Action</th>
                    <th>Queue / target</th>
                    <th>Phase / outcome</th>
                  </tr>
                </thead>
                <tbody>
                  {audit.map((event, index) => (
                    <tr key={`${event.operationId}:${event.phase}:${index}`}>
                      <td>{new Date(event.at).toLocaleString()}</td>
                      <td>{event.actorId}</td>
                      <td>{event.action}</td>
                      <td>
                        {event.queue} {event.targetId ?? ''}
                      </td>
                      <td>
                        {event.phase} {event.outcome ?? ''} {event.code ?? ''}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {!audit.length && <div className="empty">No audit events visible.</div>}
            </div>
          </section>
        </>
      )}
    </>
  );
}
