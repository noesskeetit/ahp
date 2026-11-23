import {createHash} from 'node:crypto';

const inlineLimit = 256 << 10;

export async function storeJournalJSON(client, path, epoch, value) {
  const data = Buffer.from(JSON.stringify(value), 'utf8');
  if (data.length <= inlineLimit) return {inline: data.toString('base64')};
  const ref = await client.request('POST', path + '/blobs:stage', {
    body: {lease_epoch: epoch, data: data.toString('base64')},
  });
  return {blob: ref};
}

export async function loadJournalJSON(client, path, payload) {
  if (typeof payload.inline === 'string') {
    return JSON.parse(Buffer.from(payload.inline, 'base64').toString('utf8'));
  }
  const ref = payload.blob;
  if (!ref) throw new Error('Journal record has no payload');
  const reply = await client.request('POST', path + '/blobs:read', {body: ref});
  const value = Buffer.from(reply.data, 'base64');
  const digest = createHash('sha256').update(value).digest('hex');
  if (value.length !== ref.size_bytes || digest !== ref.sha256) {
    throw new Error('Journal blob does not match its descriptor');
  }
  return JSON.parse(value.toString('utf8'));
}

export async function beginStandaloneTool(client, path, epoch, key, tool, argsDigest, turnSeq) {
  const route = path + '/tools:begin-standalone';
  const body = {lease_epoch: epoch, call_id: key, tool_name: tool, args_digest: [...argsDigest],
    ...(turnSeq !== undefined && {turn_seq: turnSeq})};
  try {
    return await client.request('POST', route, {body});
  } catch (error) {
    if (!error.outcomeUnknown) throw error;
    return await client.request('POST', route, {body: {...body, ack_lost: true}});
  }
}
