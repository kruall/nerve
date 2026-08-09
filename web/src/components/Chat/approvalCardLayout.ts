/**
 * Layout contract for an approval surface. The card stays within the viewport
 * while only its details area scrolls.
 */
export const approvalCardClassNames = {
  card: 'mx-4 mb-2 border border-hue-orange/40 rounded-lg bg-surface shadow-lg overflow-hidden flex flex-col max-h-[calc(100dvh-8rem)] min-w-0',
  header: 'px-3 py-2 flex items-center gap-2 bg-hue-orange/10 shrink-0',
  title: 'min-w-0 break-words text-[13px] font-medium text-text-primary',
  content: 'px-3 py-2 space-y-1.5 min-h-0 overflow-y-auto overscroll-contain',
  code: 'text-[12px] font-mono bg-surface-deep rounded px-2 py-1.5 whitespace-pre-wrap break-all overflow-x-hidden',
  footer: 'px-3 py-2 flex gap-2 border-t border-border shrink-0',
} as const;
