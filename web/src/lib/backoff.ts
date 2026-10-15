/**
 * Equal jitter: wait between half of `ceilingMs` and all of it. After a server restart every
 * open room tab reconnects on the same exponential schedule; without this they would all knock
 * at the same instant, and again at each later retry. The floor of half the ceiling (rather
 * than full jitter's zero) keeps a refused upgrade from being retried in a hot loop.
 * `random` returns a number in [0, 1) and is a parameter so tests can fix it.
 */
export function equalJitter(ceilingMs: number, random: () => number = Math.random): number {
  const half = ceilingMs / 2
  return half + random() * half
}
