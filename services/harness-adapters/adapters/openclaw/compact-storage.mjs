import {importStateSDK} from '../journal/sdk-source.mjs';

export const {CompactOpenClawStorage, OpenClawOutcomeUnknown, attachCompactOpenClawAgent} =
  await importStateSDK('openclaw.mjs');
