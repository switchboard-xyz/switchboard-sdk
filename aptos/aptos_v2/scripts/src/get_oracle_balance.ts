import { AptosClient, HexString } from "aptos";
import { EscrowManager } from "@switchboard-xyz/aptos.js";

// MAINNET ORACLE SERVICING PERMISSIONLESS QUEUE
const switchboard =
  "0x7d7e436f0b2aafde60774efb26ccc432cf881b677aca7faaf2a01879bd19fb8";
const queue =
  "0xc887072e37f17f9cc7afc0a00e2b283775d703c610acca3997cb26e74bc53f3b";
const oracle =
  "0xe708597ca28ebc3d3ca417494c1b428d3e8f69589faea86c5d81a6afe47cfa52";

const UNITS_PER_APT = 100_000_000;
const NODE_URL = "https://fullnode.mainnet.aptoslabs.com/v1";

(async () => {
  // get escrow manager
  const client = new AptosClient(NODE_URL);
  const escrowManager = new EscrowManager(
    client,
    HexString.ensure(oracle),
    switchboard
  );

  // get value for the oracle on the queue
  const escrow = await escrowManager.fetchItem(queue);
  console.log(
    `Oracle[${oracle}] has ${
      escrow.escrow.value.toNumber() / UNITS_PER_APT
    }APT for Queue[${queue}]`
  );
})();
