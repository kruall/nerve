import { extractResultText } from '../../../utils/extractResultText';

export interface TaskCreateResult {
  id: string;
  status: string;
}

/**
 * Extract the stable fields emitted by Nerve's task_create handler.  Results
 * may be plain text or an MCP content-block JSON array after persistence.
 */
export function parseTaskCreateResult(result?: string): TaskCreateResult | null {
  if (!result) return null;
  const match = extractResultText(result).match(/^Task created:\s+([^\s(]+)\s+\(status:\s*([^)]+)\)/m);
  if (!match) return null;
  return { id: match[1], status: match[2].trim() };
}
