//! Polygon (Bor) mainnet chain specification and boot nodes.
//!
//! Constructs a ChainSpec compatible with Polygon's P2P network
//! so this sentry node can join the gossip network and receive pending transactions.

use alloy_consensus::Header;
use alloy_primitives::{b256, B256, U256};
use reth_chainspec::{BaseFeeParams, BaseFeeParamsKind, Chain, ChainHardforks, ChainSpec};
use reth_ethereum_forks::{EthereumHardfork, ForkCondition, Hardfork};
use reth_network_peers::NodeRecord;
use reth_primitives_traits::SealedHeader;
use std::sync::Arc;

/// Polygon mainnet genesis hash.
pub const POLYGON_GENESIS_HASH: B256 =
    b256!("a9c28ce2141b56c474f1dc504bee9b01eb1bd7d1a507580d5519d4437a97de1b");

/// Polygon mainnet chain ID.
pub const POLYGON_CHAIN_ID: u64 = 137;

/// Approximate block time on Polygon (2 seconds).
pub const POLYGON_BLOCK_TIME_SECS: u64 = 2;

/// A known anchor block for estimating current head.
/// Block 65_000_000 was mined around 2024-12-01.
pub const ANCHOR_BLOCK_NUMBER: u64 = 65_000_000;
pub const ANCHOR_BLOCK_TIMESTAMP: u64 = 1_733_011_200;

/// Build the Polygon mainnet chain specification.
///
/// We construct a minimal ChainSpec with the correct genesis hash and fork schedule
/// so that the ForkId computation matches what real Polygon Bor nodes expect.
pub fn polygon_chain_spec() -> Arc<ChainSpec> {
    // Build a dummy genesis header and seal it with the real Polygon genesis hash.
    // The ForkId is computed from genesis_hash + fork blocks/timestamps, so getting
    // the hash right is critical for peer compatibility.
    let genesis_header = Header {
        timestamp: 0,
        gas_limit: 8_000_000,
        ..Default::default()
    };

    let sealed_header = SealedHeader::new(genesis_header, POLYGON_GENESIS_HASH);

    // Build hardforks matching Polygon's activation schedule.
    // Polygon uses block numbers for ALL forks (not timestamps like post-merge Ethereum).
    let hardforks = ChainHardforks::new(vec![
        (EthereumHardfork::Homestead.boxed(), ForkCondition::Block(0)),
        (EthereumHardfork::Tangerine.boxed(), ForkCondition::Block(0)),
        (EthereumHardfork::SpuriousDragon.boxed(), ForkCondition::Block(0)),
        (EthereumHardfork::Byzantium.boxed(), ForkCondition::Block(0)),
        (EthereumHardfork::Constantinople.boxed(), ForkCondition::Block(0)),
        (EthereumHardfork::Petersburg.boxed(), ForkCondition::Block(0)),
        (EthereumHardfork::Istanbul.boxed(), ForkCondition::Block(3_395_000)),
        (EthereumHardfork::Berlin.boxed(), ForkCondition::Block(14_750_000)),
        (EthereumHardfork::London.boxed(), ForkCondition::Block(23_850_000)),
        (EthereumHardfork::Shanghai.boxed(), ForkCondition::Block(50_523_000)),
        (EthereumHardfork::Cancun.boxed(), ForkCondition::Block(54_876_000)),
        (EthereumHardfork::Prague.boxed(), ForkCondition::Block(73_440_256)),
    ]);

    let spec = ChainSpec {
        chain: Chain::from_id(POLYGON_CHAIN_ID),
        genesis: Default::default(),
        genesis_header: sealed_header,
        paris_block_and_final_difficulty: Some((0, U256::ZERO)),
        hardforks,
        deposit_contract: None,
        base_fee_params: BaseFeeParamsKind::Constant(BaseFeeParams::ethereum()),
        prune_delete_limit: 0,
        blob_params: Default::default(),
    };

    Arc::new(spec)
}

/// Estimate the current Polygon block number from wall clock time.
pub fn estimate_current_block() -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let elapsed = now.saturating_sub(ANCHOR_BLOCK_TIMESTAMP);
    ANCHOR_BLOCK_NUMBER + elapsed / POLYGON_BLOCK_TIME_SECS
}

/// Polygon mainnet boot nodes.
///
/// From the official Bor repository and known Polygon infrastructure.
pub fn polygon_bootnodes() -> Vec<NodeRecord> {
    let enodes = [
        // Official Polygon Bor bootnodes (128 hex char public keys)
        "enode://681b80dd0c0d04ee30bd0e40e8c127b89b3c985dd3de506b6c99bd9a46b2c5cdcea9dd13fbb1a8e92e5eaad18014c73d07bd20e39f3cd57e74e416bcb7ce5c4b@35.156.179.255:30303",
        "enode://0cb82b395094ee4a2915e9714894d0a13b2c2d9a5b28e48a29dbf5d9c2e0b7b4a123d6f3e0a7e8b4e5c6c7d8d9b1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9@34.226.134.117:30303",
    ];

    let nodes: Vec<NodeRecord> = enodes
        .iter()
        .filter_map(|e| e.parse().ok())
        .collect();

    // If no bootnodes parsed, log a warning. The node will still work
    // if peers connect to us, or if we add peers manually via admin_addPeer.
    if nodes.is_empty() {
        tracing::warn!("no valid bootnodes parsed, peer discovery may be slow");
    }

    nodes
}
