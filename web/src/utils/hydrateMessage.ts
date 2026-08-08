import type { ChatMessage, MessageBlock } from '../types/chat';
import { upsertToolCallBlock } from '../stores/helpers/blockHelpers';

export function hydrateMessage(raw: any): ChatMessage {
  if (raw.role === 'user') {
    const userBlocks: MessageBlock[] = [{ type: 'text', content: raw.content || '' }];
    // Restore image/file blocks from DB (uploaded files)
    if (raw.blocks && Array.isArray(raw.blocks)) {
      for (const b of raw.blocks) {
        if (b.type === 'image') {
          userBlocks.push({ type: 'image', url: b.url || '', filename: b.filename || '', media_type: b.media_type || '' });
        } else if (b.type === 'file') {
          userBlocks.push({ type: 'file', url: b.url || '', filename: b.filename || '', size: b.size });
        }
      }
    }
    return {
      id: raw.id,
      role: 'user',
      blocks: userBlocks,
      channel: raw.channel,
      created_at: raw.created_at,
    };
  }

  // Assistant messages always carry an ordered `blocks` array (V26
  // migration backfilled any legacy rows that only had the dropped
  // `tool_calls` column).
  const rawBlocks = Array.isArray(raw.blocks) ? raw.blocks : [];
  const blocks: MessageBlock[] = rawBlocks.reduce((hydrated: MessageBlock[], b: any) => {
    if (b.type === 'thinking') {
      hydrated.push({ type: 'thinking' as const, content: b.content || '' });
      return hydrated;
    }
    if (b.type === 'tool_call') {
      return upsertToolCallBlock(hydrated, {
        type: 'tool_call' as const,
        toolUseId: b.tool_use_id || '',
        tool: b.tool,
        input: b.input || {},
        result: b.result,
        isError: b.is_error,
        status: 'complete' as const,
        // Dynamic-workflow snapshot, folded into the block by the backend
        // (merge_workflow_into_call) so the panel reconstructs after reload.
        workflow: b.workflow,
      });
    }
    if (b.type === 'image') {
      hydrated.push({ type: 'image' as const, url: b.url || '', filename: b.filename || '', media_type: b.media_type || '' });
      return hydrated;
    }
    if (b.type === 'file') {
      hydrated.push({ type: 'file' as const, url: b.url || '', filename: b.filename || '', size: b.size });
      return hydrated;
    }
    if (b.type === 'wakeup') {
      hydrated.push({ type: 'wakeup' as const });
      return hydrated;
    }
    if (b.type === 'auto') {
      hydrated.push({ type: 'auto' as const });
      return hydrated;
    }
    if (b.type === 'model_change') {
      hydrated.push({ type: 'model_change' as const, from: b.from, to: b.to || '', downgrade: b.downgrade });
      return hydrated;
    }
    // Default: text
    hydrated.push({ type: 'text' as const, content: b.content || '' });
    return hydrated;
  }, []);

  // If a row somehow has neither blocks nor any reconstructed content
  // (shouldn't happen post-V26), fall back to whatever raw.content is so
  // we don't render a completely empty message.
  if (blocks.length === 0 && raw.content) {
    blocks.push({ type: 'text', content: raw.content });
  }

  return {
    id: raw.id,
    role: 'assistant',
    blocks,
    channel: raw.channel,
    created_at: raw.created_at,
  };
}
