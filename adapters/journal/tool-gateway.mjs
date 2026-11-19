async function lookup(gateway, key, tool, allowMissing) {
  let result;
  try {
    result = await gateway('GetResult', {key});
  } catch (error) {
    if (allowMissing && error.status === 404 && error.code === 'NOT_FOUND') return {kind: 'missing'};
    if (error.status !== 409) throw error;
    const operation = await gateway('Get', {key});
    if (operation.key !== key || operation.target !== tool) {
      throw new Error('Tool Gateway returned another operation');
    }
    if (operation.status === 'SIDE_EFFECT_UNKNOWN') return {kind: 'unknown'};
    throw new Error(`Tool recovery is waiting on Gateway status ${operation.status}`);
  }
  if (result.key !== key || result.target !== tool || result.status !== 'SUCCEEDED' ||
      result.result_storage !== 'inline' || !Array.isArray(result.payload?.content)) {
    throw new Error('Unsupported Tool Gateway receipt');
  }
  return {kind: 'known', payload: result.payload};
}

/** Resolve a persisted intent before the native agent loop starts. */
export async function settleGatewayCall(gateway, {key, tool, getArguments, prepared, markDispatched}) {
  const result = await lookup(gateway, key, tool, true);
  if (result.kind === 'missing' || prepared) await markDispatched();
  if (result.kind !== 'missing') return result;
  try {
    await gateway('Call', {key, tool, arguments: getArguments()});
  } catch (error) {
    if (error.code !== 'TRANSPORT') throw error;
  }
  return await lookup(gateway, key, tool, false);
}
