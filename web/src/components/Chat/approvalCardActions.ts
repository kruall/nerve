export function approvalCardActions(
  answerInteraction: (result: Record<string, string> | null) => void,
  denyInteraction: (message?: string) => void,
) {
  return {
    approve: () => answerInteraction(null),
    decline: () => denyInteraction('Declined by user.'),
  };
}
