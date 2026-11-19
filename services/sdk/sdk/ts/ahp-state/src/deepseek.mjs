export async function loadDeepSeekSessionPersistence() {
  const {default: CompactSessionPersistence} = await import('./deepseek/compact-session-persistence.mjs');
  return CompactSessionPersistence;
}

export async function createDeepSeekSessionPersistence(ctx, options) {
  const CompactSessionPersistence = await loadDeepSeekSessionPersistence();
  return new CompactSessionPersistence(ctx, options);
}
