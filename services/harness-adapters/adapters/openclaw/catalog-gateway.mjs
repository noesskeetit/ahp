import {importStateSDK} from '../journal/sdk-source.mjs';

export const {createOpenClawGatewayCatalog, listJournalSessions, journalSessionHealth} =
  await importStateSDK('openclaw/catalog-gateway.mjs');
