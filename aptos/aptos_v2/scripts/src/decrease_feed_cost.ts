import { AptosClient, AptosAccount, HexString } from "aptos";
import { OracleQueueAccount } from "@switchboard-xyz/aptos.js";
import * as YAML from "yaml";
import * as fs from "fs";

const NODE_URL =
  "https://aptos-api.rpcpool.com/8f545350616dc47e67cfb35dc857/v1";

const SWITCHBOARD_ADDRESS =
  "0x7d7e436f0b2aafde60774efb26ccc432cf881b677aca7faaf2a01879bd19fb8";

// QUEUES TO RECONFIGURE
const PERMISSIONLESS_QUEUE_ADDRESS =
  "0xc887072e37f17f9cc7afc0a00e2b283775d703c610acca3997cb26e74bc53f3b";

(async () => {
  const client = new AptosClient(NODE_URL);

  let funder;

  // if file extension ends with yaml
  try {
    const parsedYaml = YAML.parse(
      fs.readFileSync("../.aptos/config.yaml", "utf8")
    );
    funder = new AptosAccount(
      HexString.ensure(
        parsedYaml!.profiles!.queue_authority!.private_key!
      ).toUint8Array()
    );
  } catch (e) {
    console.log(e);
  }

  if (!funder) {
    throw new Error("Could not get funder account.");
  }

  const THIRTY_MINUTES = 30 * 60;

  /// PERMISSIONLESS QUEUE RECONFIGURATION
  try {
    const permissionlessQueue = new OracleQueueAccount(
      client,
      PERMISSIONLESS_QUEUE_ADDRESS,
      SWITCHBOARD_ADDRESS
    );
    const queueData = await permissionlessQueue.loadData();

    await permissionlessQueue.setConfigs(funder, {
      oracleTimeout: THIRTY_MINUTES,

      name: Buffer.from(queueData.name).toString("utf8"),
      metadata: Buffer.from(queueData.metadata).toString("utf8"),
      authority: queueData.authority,
      reward: 1665, // THE ONLY THING THAT ISN'T THE SAME <<<----------- 37 (gas) * 15 (gas price) -> 555 * 3 (wiggle room) -> 1665 octas
      minStake: queueData.minStake.toNumber(),
      slashingEnabled: queueData.slashingEnabled,
      varianceToleranceMultiplierValue:
        queueData.varianceToleranceMultiplier.value.toNumber(),
      varianceToleranceMultiplierScale:
        queueData.varianceToleranceMultiplier.dec,
      feedProbationPeriod: queueData.feedProbationPeriod.toNumber(),
      consecutiveFeedFailureLimit:
        queueData.consecutiveFeedFailureLimit.toNumber(),
      consecutiveOracleFailureLimit:
        queueData.consecutiveOracleFailureLimit.toNumber(),
      unpermissionedFeedsEnabled: queueData.unpermissionedFeedsEnabled,
      unpermissionedVrfEnabled: queueData.unpermissionedVrfEnabled,
      lockLeaseFunding: queueData.lockLeaseFunding,
      enableBufferRelayers: queueData.enableBufferRelayers,
      maxSize: queueData.maxSize.toNumber(),
      coinType: "0x1::aptos_coin::AptosCoin",
    });
    console.log(`${PERMISSIONLESS_QUEUE_ADDRESS} done`);
  } catch (e) {
    console.log(e);
  }
})();
