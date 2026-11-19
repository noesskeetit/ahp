import {registerOpenClawGatewayCatalog} from './catalog-loader.mjs';

registerOpenClawGatewayCatalog({
  installDir: process.env.AHP_OPENCLAW_INSTALL_DIR,
  handshake: process.env.AHP_OPENCLAW_CATALOG_HANDSHAKE,
});
