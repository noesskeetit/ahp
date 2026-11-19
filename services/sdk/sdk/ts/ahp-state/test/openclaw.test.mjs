import assert from 'node:assert/strict';
import test from 'node:test';
import {CompactOpenClawStorage} from '../src/openclaw/compact-storage.mjs';

function journalRow(seq, id, type, value) {
  return {seq, id, type, payload: {inline: Buffer.from(JSON.stringify(value)).toString('base64')}};
}

test('an open read-only tree hydrates only new rows and releases its projection on close', async () => {
  const rows = [journalRow(1, 'oc:header', 'openclaw.header', {id: 'session'})];
  const readCursors = [];
  const client = {async request(_method, _path, {query}) {
    readCursors.push(query.after_seq ?? 0);
    const leaf = rows[1]?.id;
    return {head_seq: rows.length, leaf_entry_id: leaf, append_parent_id: leaf,
      events: rows.filter(row => row.seq > (query.after_seq ?? 0))};
  }};
  class NativeStorage {
    constructor({entries = []}) { this.entries = entries; }
    async appendEntry(entry) { this.entries.push(entry); }
    async getEntries() { return [...this.entries]; }
    async getLeafId() { return this.entries.at(-1)?.id ?? null; }
    async getAppendParentId() { return this.getLeafId(); }
  }
  const storage = await new CompactOpenClawStorage({InMemorySessionStorage: NativeStorage}, client, 'session')
    .open({readOnly: true});
  const message = {id: 'next', type: 'message', parentId: null};
  rows.push(journalRow(2, 'oc:entry:next', 'openclaw.entry', message));
  assert.deepEqual(await storage.getEntries(), [message]);
  assert.deepEqual(readCursors, [0, 1]);
  await storage.close();
  await assert.rejects(storage.getEntries(), /closed/);
});
