import { describe, expect, it } from 'vitest';
import { hydrateMessage } from './hydrateMessage';

describe('hydrateMessage', () => {
  it('keeps one persisted task_create card per tool_use_id', () => {
    const message = hydrateMessage({
      id: 1,
      role: 'assistant',
      blocks: [
        { type: 'tool_call', tool_use_id: 'call-1', tool: 'mcp__nerve__task_create', input: { title: 'Fix card' } },
        { type: 'tool_call', tool_use_id: 'call-1', tool: 'mcp__nerve__task_create', input: { title: 'Fix card' }, result: 'Task created: UI-12 (status: pending)' },
      ],
    });
    expect(message.blocks).toHaveLength(1);
    expect(message.blocks[0]).toMatchObject({ toolUseId: 'call-1', result: 'Task created: UI-12 (status: pending)' });
  });
});
