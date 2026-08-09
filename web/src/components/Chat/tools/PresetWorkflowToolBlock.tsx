import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { CircleStop, Clock3, Loader2, Workflow } from 'lucide-react';
import type { ToolCallBlockData } from '../../../types/chat';
import { api, type PresetWorkflow } from '../../../api/client';
import { ws } from '../../../api/websocket';

const ACTIVE = new Set(['queued', 'running', 'cancelling']);

function statusClass(status: string): string {
  if (status === 'succeeded') return 'border-emerald-400/25 bg-emerald-400/10 text-hue-emerald';
  if (status === 'failed') return 'border-red-400/25 bg-red-400/10 text-hue-red';
  if (status === 'cancelled') return 'border-border bg-surface-raised text-text-muted';
  return 'border-blue-400/25 bg-blue-400/10 text-hue-blue';
}

function duration(start?: string | null, finish?: string | null): string {
  if (!start) return 'waiting';
  const seconds = Math.max(0, Math.floor(((finish ? Date.parse(finish) : Date.now()) - Date.parse(start)) / 1000));
  return seconds < 60 ? `${seconds}s` : `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
}

/** A redacted preset-workflow view.  Details always come from REST, never WS. */
export function PresetWorkflowToolBlock({ block: _block, workflowId }: { block: ToolCallBlockData; workflowId: string }) {
  const [workflow, setWorkflow] = useState<PresetWorkflow | null>(null);
  const [missing, setMissing] = useState(false);
  const [cancelling, setCancelling] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let disposed = false;
    const load = async () => {
      try {
        const next = await api.getPresetWorkflow(workflowId);
        if (!disposed) { setWorkflow(next); setMissing(false); }
      } catch {
        if (!disposed) setMissing(true);
      }
    };
    void load();
    const unsubscribe = ws.onMessage(message => {
      if (message.type === 'workflow_update' && message.workflow.id === workflowId) void load();
    });
    return () => { disposed = true; unsubscribe(); };
  }, [workflowId]);

  const cancel = async () => {
    if (!workflow || !window.confirm(`Cancel preset workflow ${workflow.id}?`)) return;
    setCancelling(true); setError(null);
    try { setWorkflow(await api.cancelPresetWorkflow(workflow.id)); }
    catch (e) { setError(e instanceof Error ? e.message : String(e)); }
    finally { setCancelling(false); }
  };

  if (missing) return <div className="my-1.5 border border-border rounded-lg bg-surface px-3 py-2 text-[12px] text-text-muted">Preset workflow <span className="font-mono">{workflowId}</span> is no longer available.</div>;
  if (!workflow) return <div className="my-1.5 border border-border rounded-lg bg-surface px-3 py-2 flex gap-2 items-center text-[13px] text-text-secondary"><Loader2 size={14} className="animate-spin text-text-muted"/> Preset workflow <span className="font-mono text-text-faint">{workflowId}</span></div>;

  const active = ACTIVE.has(workflow.status);
  const terminalError = workflow.status === 'failed' ? String(workflow.result?.error ?? workflow.result?.outcome ?? 'Workflow failed.') : '';
  const terminalResult = workflow.status === 'succeeded' ? String(workflow.result?.outcome ?? 'succeeded') : '';
  return <div className="my-1.5 border border-border rounded-lg bg-surface overflow-hidden">
    <div className="flex items-center gap-2 px-3 py-2 min-w-0">
      <Workflow size={14} className="text-hue-violet shrink-0"/>
      <span className={`inline-flex px-1.5 py-0.5 rounded-full border text-[10px] capitalize ${statusClass(workflow.status)}`}>{workflow.status}</span>
      <span className="text-[13px] font-medium text-text-secondary truncate">{workflow.preset.title}</span>
      <span className="font-mono text-[11px] text-text-faint shrink-0">{workflow.id}</span>
      {active && <button type="button" onClick={() => void cancel()} disabled={cancelling} className="ml-auto inline-flex items-center gap-1 px-2 py-1 rounded border border-red-400/25 text-hue-red text-[11px] disabled:opacity-50 cursor-pointer">{cancelling ? <Loader2 size={11} className="animate-spin"/> : <CircleStop size={11}/>} Cancel</button>}
    </div>
    <div className="px-3 pb-2 flex items-center gap-1.5 text-[11px] text-text-dim"><Clock3 size={11}/>{duration(workflow.started_at, workflow.finished_at)} · {workflow.preset.name} v{workflow.preset.version}</div>
    <div className="border-t border-border-subtle px-3 py-2 space-y-1">
      {workflow.stages.map(stage => <div key={stage.id} className="flex items-center gap-2 text-[11px] min-w-0"><span className="font-mono text-text-secondary">{stage.stage_id}</span><span className="truncate text-text-faint">{stage.runner === 'agent' ? `${stage.runtime?.model ?? 'agent'} · ${stage.runtime?.effort ?? 'default'}` : stage.runtime?.kind ?? 'execution'}</span><span className="ml-auto text-text-muted">{stage.status}</span>{stage.child_id && <Link to={stage.child_type === 'agent' ? '/workflow-runs' : `/chat/${workflow.owner_session_id}`} title={stage.child_id} className="text-accent shrink-0">{stage.child_type === 'agent' ? 'run' : 'session'}</Link>}</div>)}
      {!workflow.stages.length && <div className="text-[11px] text-text-faint">Stages are being prepared…</div>}
    </div>
    {terminalError && <div className="px-3 pb-2 text-[11px] text-hue-red break-words">{terminalError}</div>}
    {terminalResult && <div className="px-3 pb-2 text-[11px] text-text-muted">Result: {terminalResult}</div>}
    {error && <div className="px-3 pb-2 text-[11px] text-hue-red break-words">{error}</div>}
    <div className="border-t border-border-subtle px-3 py-1.5 flex gap-3 text-[11px]"><Link to="/workflows" className="text-text-dim hover:text-text-secondary">Preset workflows</Link><Link to={`/chat/${workflow.owner_session_id}`} className="text-text-dim hover:text-text-secondary">Open session</Link></div>
  </div>;
}
