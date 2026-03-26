/**
 * Switchboard EVM contract ABI for interacting with oracle price feeds.
 *
 * This is a minimal human-readable ABI that can be used with ethers.js, viem,
 * or other EVM libraries that support the human-readable format.
 */
export const SWITCHBOARD_ABI = [
  'function updatePrices(bytes[] calldata updates, bytes32[] calldata feedIds) external payable',
  'function getPrice(bytes32 feedId) external view returns (int128 value, uint256 timestamp, uint64 slotNumber)',
  'event PriceUpdated(bytes32 indexed feedId, int128 oldPrice, int128 newPrice, uint256 timestamp, uint64 slotNumber)',
] as const;
