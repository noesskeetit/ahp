import {existsSync} from 'node:fs';

const source = new URL('../../../sdk/sdk/ts/ahp-state/src/', import.meta.url);
const assembled = new URL('../../sdk/ts/ahp-state/src/', import.meta.url);
const sdk = existsSync(new URL('index.mjs', source)) ? source : assembled;

export function importStateSDK(path) {
  return import(new URL(path, sdk));
}
