// Queue control and SDK completion rows are outside the native session format.
export function isTurnControl(record) {
  if (record.codec !== 'json' || record.parent_id) return false;
  if (record.type === 'turn.output') {
    return record.role === 'assistant' && Number.isSafeInteger(record.turn_seq) && record.turn_seq > 0 &&
      record.id === `turn.output:${record.turn_seq}`;
  }
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

// Compare the wire deadlines without losing the precision beyond milliseconds
// in the signed lease. Date.parse accounts for the RFC3339 timezone offset.
function compareDeadline(left, right) {
  const delta = Date.parse(left) - Date.parse(right);
  if (delta !== 0) return delta;
  const fraction = value => (value.match(/\.(\d+)(?:Z|[+-]\d{2}:\d{2})$/)?.[1] ?? '').padEnd(9, '0');
  return Number(fraction(left)) - Number(fraction(right));
}

export class VMLease {
  #turn;

  constructor(turn) { this.#turn = {...turn}; }

  get current() { return this.#turn && {...this.#turn}; }

  applyHeartbeat(renewed) {
    const current = this.#turn;
    if (!current || !renewed || current.epoch !== renewed.epoch || current.worker !== renewed.worker) return;
    const order = compareDeadline(renewed.expires_at, current.expires_at);
    if (!Number.isFinite(order) || order < 0) return;
    if (order > 0 || renewed.tool_lease_proof) {
      this.#turn = {...current, expires_at: renewed.expires_at, tool_lease_proof: renewed.tool_lease_proof};
    }
    if (renewed.control?.turn_seq === current.input_seq) return renewed.control;
  }

  applyFinish(finishedTurnSeq, receipt) {
    const current = this.#turn;
    if (!current || current.input_seq !== finishedTurnSeq) return false;
    const control = receipt.control;
    const next = control?.next_turn;
    if (!next) {
      this.#turn = undefined;
      return false;
    }
    if (next.session_id !== current.session_id || next.worker !== current.worker || next.epoch !== current.epoch ||
        next.input_seq <= finishedTurnSeq || next.input_seq !== control.turn_seq) return false;
    const order = compareDeadline(next.expires_at, current.expires_at);
    if (!Number.isFinite(order)) return false;
    this.#turn = order < 0 || (order === 0 && !control.tool_lease_proof)
      ? {...next, expires_at: current.expires_at, tool_lease_proof: current.tool_lease_proof}
      : {...next, tool_lease_proof: control.tool_lease_proof};
    return true;
  }
}
