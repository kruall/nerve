import { describe, expect, it } from 'vitest';
import { parseTaskCreateResult } from './taskCreateResult';

describe('parseTaskCreateResult', () => {
  it('parses the current task_create result', () => {
    expect(parseTaskCreateResult('Task created: UI-12 (status: pending)\nFile: memory/tasks/UI-12.md'))
      .toEqual({ id: 'UI-12', status: 'pending' });
  });

  it('parses persisted MCP text content', () => {
    expect(parseTaskCreateResult(JSON.stringify([{ type: 'text', text: 'Task created: UI-12 (status: in-progress)' }])))
      .toEqual({ id: 'UI-12', status: 'in-progress' });
  });

  it('safely rejects legacy or malformed results', () => {
    expect(parseTaskCreateResult('created successfully')).toBeNull();
  });
});
