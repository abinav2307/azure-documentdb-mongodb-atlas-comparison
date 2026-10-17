export const SKU_TIERS = Object.freeze(["M10", "M20", "M30"]);
export const INITIAL_SKUS = Object.freeze(["M10", "M20"]);
export const STORAGE_SIZE_GB = 32;

export function higherSkus(initialTier) {
  if (!INITIAL_SKUS.includes(initialTier)) {
    throw new Error(`Unsupported initial compute tier: ${initialTier}`);
  }
  return [SKU_TIERS[SKU_TIERS.indexOf(initialTier) + 1]];
}

export function validateComputeTransition(initialTier, targetTier) {
  if (!higherSkus(initialTier).includes(targetTier)) {
    throw new Error(`Unapproved SKU transition: ${initialTier} -> ${targetTier}; only M10 -> M20 and M20 -> M30 are allowed`);
  }
}

export const WORKLOAD_THREADS_BY_SKU = Object.freeze({
  M10: 1,
  M20: 1,
  M30: 2,
  M40: 4,
  M50: 8,
  M60: 16,
  M80: 32
});

export function workloadThreads(initialTier) {
  const threads = WORKLOAD_THREADS_BY_SKU[initialTier];
  if (!threads) throw new Error(`Unsupported initial compute tier: ${initialTier}`);
  return threads;
}
