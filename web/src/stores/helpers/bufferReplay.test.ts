import { describe, expect, it } from 'vitest';
import type { WSMessage } from '../../api/websocket';
import { applyStreamEvent } from './bufferReplay';

describe('applyStreamEvent tool identity', () => {
  it('keeps one task_create card across repeated tool_use and tool_result events', () => {
    const events: WSMessage[] = [
      { type: 'tool_use', session_id: 's', tool: 'mcp__nerve__task_create', input: { title: 'Fix card' }, tool_use_id: 'call-1' },
      { type: 'tool_result', session_id: 's', tool_use_id: 'call-1', result: 'Task created: UI-12 (status: pending)' },
      { type: 'tool_use', session_id: 's', tool: 'mcp__nerve__task_create', input: { title: 'Fix card' }, tool_use_id: 'call-1' },
      { type: 'tool_result', session_id: 's', tool_use_id: 'call-1', result: 'Task created: UI-12 (status: pending)' },
    ];
    const blocks = events.reduce(applyStreamEvent, []);
    expect(blocks).toHaveLength(1);
    expect(blocks[0]).toMatchObject({ toolUseId: 'call-1', status: 'complete' });
  });
});
