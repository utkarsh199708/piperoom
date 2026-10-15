/** The steer that asks the agent to propose the undo. Title and rollback are agent text already on the card. */
export function rollbackSteer(title: string, proposalId: string, rollback: string): string {
  return `Roll back proposal "${title}" (${proposalId}): propose a fix that undoes it. Recorded rollback: ${rollback}`
}
