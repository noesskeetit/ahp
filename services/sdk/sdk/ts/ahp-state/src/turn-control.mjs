// Queue control rows occupy the journal sequence but are not native session records.
export function isTurnControl(record) {
  if (record.codec !== 'json' || record.parent_id) return false;
  if (record.type === 'turn.completed') {
    return (record.role === 'assistant' || (record.role === 'system' && record.turn_seq > 0)) &&
      /^turn\.completed:[1-9][0-9]*$/.test(record.id);
  }
  if (record.type === 'ahp.state.mutation') {
    return record.role === 'system' && record.turn_seq > 0 && /^state-mutation-[0-9a-f]{64}$/.test(record.id);
  }
  return (record.type === 'turn.input' || record.type === 'turn.steer' || record.type === 'turn.followup') &&
    record.role === 'user' && /^turn\.input:[0-9a-f]{64}$/.test(record.id);
}

export async function assertActiveTurn(client, path, sessionId, turn, signal) {
  if (turn.session_id !== sessionId || !Number.isSafeInteger(turn.epoch) || turn.epoch <= 0 ||
      !Number.isSafeInteger(turn.input_seq) || turn.input_seq <= 0 ||
      !Number.isSafeInteger(turn.head_seq) || turn.head_seq < turn.input_seq ||
      (turn.turn_seq !== undefined && turn.turn_seq !== turn.input_seq)) {
    throw new TypeError('Expected a preclaimed turn for this session');
  }
  const current = await client.request('GET', path, {signal});
  if (current.status !== 'ACTIVE' || current.lease_epoch !== turn.epoch ||
      current.input_seq !== turn.input_seq || !Number.isSafeInteger(current.head_seq) ||
      current.head_seq < turn.head_seq) {
    throw new Error('Preclaimed turn is not active at the supplied lease epoch');
  }
}
