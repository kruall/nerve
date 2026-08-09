import { describe, expect, it } from 'vitest';
import { approvalCardActions } from './approvalCardActions';
import { approvalCardClassNames } from './approvalCardLayout';

describe('ApprovalCard layout', () => {
  it('keeps the controls outside a viewport-bounded scrolling content region', () => {
    expect(approvalCardClassNames.card).toContain('flex-col');
    expect(approvalCardClassNames.card).toContain('max-h-[calc(100dvh-8rem)]');
    expect(approvalCardClassNames.content).toContain('min-h-0');
    expect(approvalCardClassNames.content).toContain('overflow-y-auto');
    expect(approvalCardClassNames.content).toContain('overscroll-contain');
    expect(approvalCardClassNames.header).toContain('shrink-0');
    expect(approvalCardClassNames.footer).toContain('shrink-0');
  });

  it('wraps an unbroken JSON argument value without horizontal overflow', () => {
    expect(approvalCardClassNames.code).toContain('whitespace-pre-wrap');
    expect(approvalCardClassNames.code).toContain('break-all');
    expect(approvalCardClassNames.code).toContain('overflow-x-hidden');
  });

  it('keeps approval actions wired to the existing interaction handlers', () => {
    const calls: unknown[] = [];
    const actions = approvalCardActions(
      result => calls.push(['approve', result]),
      message => calls.push(['decline', message]),
    );

    actions.approve();
    actions.decline();

    expect(calls).toEqual([
      ['approve', null],
      ['decline', 'Declined by user.'],
    ]);
  });
});
