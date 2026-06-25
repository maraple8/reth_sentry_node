//! P2P network setup for the sentry node.
//!
//! Sets up the devp2p networking layer using reth components:
//! - Discv4 for peer discovery
//! - RLPx for encrypted communication
//! - eth protocol for transaction gossip
//! - NewBlock caching and rebroadcast for peer block serving

use crate::block_cache::BlockCache;
use crate::block_import::CachingBlockImport;
use crate::eth_proxy;
use crate::forwarder::{ForwardableTx, TxForwarder};
use crate::polygon;
use alloy_eips::Encodable2718;
use alloy_primitives::U256;
use reth_chainspec::MAINNET;
use reth_eth_wire::EthNetworkPrimitives;
use reth_ethereum_forks::Head;
use reth_network::message::{NewBlockMessage, PeerMessage};
use reth_network::{config::SecretKey, NetworkConfigBuilder, NetworkManager, NetworkHandle};
use reth_network_api::{
    events::PeerEvent, BlockDownloaderProvider, NetworkEvent, NetworkEventListenerProvider,
    Peers, PeersInfo,
};
use reth_transaction_pool::{
    blobstore::InMemoryBlobStore, CoinbaseTipOrdering, EthPooledTransaction, Pool,
    PoolTransaction, TransactionPool,
};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::validator::{StatelessValidator, StatelessValidatorConfig};

/// Configuration for the sentry network.
#[derive(Debug, Clone)]
pub struct SentryNetworkConfig {
    /// Chain ID (137 = Polygon mainnet).
    pub chain_id: u64,
    /// Maximum number of peers.
    pub max_peers: u32,
    /// Port to listen on for P2P.
    pub p2p_port: u16,
    /// Port for discovery (UDP).
    pub discovery_port: u16,
    /// Number of recent blocks to cache.
    pub block_cache_size: usize,
    /// Additional bootnodes (enode:// URIs).
    pub bootnodes: Vec<String>,
}

impl Default for SentryNetworkConfig {
    fn default() -> Self {
        Self {
            chain_id: 137,
            max_peers: 50,
            p2p_port: 30303,
            discovery_port: 30303,
            block_cache_size: 256,
            bootnodes: vec![],
        }
    }
}

/// Type alias for our transaction pool.
pub type SentryTxPool =
    Pool<StatelessValidator, CoinbaseTipOrdering<EthPooledTransaction>, InMemoryBlobStore>;

/// Start the sentry P2P network.
///
/// Caches NewBlock announcements from peers, rebroadcasts them to all
/// connected peers, and responds to GetBlockHeaders/GetBlockBodies.
/// Runs until the `shutdown` token is cancelled.
pub async fn start_sentry_network(
    net_config: SentryNetworkConfig,
    forwarder: Arc<TxForwarder>,
    secret_key: SecretKey,
    shutdown: CancellationToken,
    data_dir: PathBuf,
) -> eyre::Result<()> {
    info!("starting sentry node on port {}", net_config.p2p_port);

    // Create the stateless validator
    let validator_config = StatelessValidatorConfig {
        chain_id: net_config.chain_id,
        ..Default::default()
    };
    let validator = StatelessValidator::new(validator_config);

    // Create the transaction pool with our stateless validator
    let blob_store = InMemoryBlobStore::default();
    let tx_pool: SentryTxPool = Pool::new(
        validator,
        CoinbaseTipOrdering::default(),
        blob_store,
        Default::default(),
    );

    info!("transaction pool created");

    // Create block cache and restore from disk if available
    let block_cache = BlockCache::new(net_config.block_cache_size);
    let cache_path = data_dir.join("block_cache.bin");
    if let Err(e) = block_cache.load_from_file(&cache_path) {
        warn!("failed to load block cache: {}", e);
    }

    // Create rebroadcast channel and block import
    let (rebroadcast_tx, rebroadcast_rx) = mpsc::unbounded_channel();
    let block_import = CachingBlockImport::new(block_cache.clone(), rebroadcast_tx);

    // Build network config using noop provider (no block sync)
    let peers_config = reth_network::PeersConfig::default()
        .with_max_outbound(net_config.max_peers as usize / 2)
        .with_max_inbound_opt(Some(net_config.max_peers as usize / 2));

    // Determine chain spec and head block based on chain_id
    let is_polygon = net_config.chain_id == polygon::POLYGON_CHAIN_ID;

    // Determine head block for status message and fork ID.
    // Use latest cached block if available, otherwise estimate from current time.
    let head = if let Some((hash, header)) = block_cache.latest() {
        info!(
            block_number = header.number,
            %hash,
            "using cached block as head"
        );
        Head {
            number: header.number,
            hash,
            difficulty: U256::ZERO,
            total_difficulty: U256::ZERO,
            timestamp: header.timestamp,
        }
    } else {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        if is_polygon {
            let estimated_number = polygon::estimate_current_block();
            info!(
                estimated_number,
                "no cached blocks, using estimated Polygon head"
            );
            Head {
                number: estimated_number,
                hash: polygon::POLYGON_GENESIS_HASH,
                difficulty: U256::ZERO,
                total_difficulty: U256::ZERO,
                timestamp: now,
            }
        } else {
            // Ethereum mainnet estimation
            let estimated_number = (now - 1_681_338_455) / 12 + 17_034_870;
            info!(
                estimated_number,
                "no cached blocks, using estimated head for fork ID"
            );
            Head {
                number: estimated_number,
                hash: MAINNET.genesis_hash(),
                difficulty: U256::ZERO,
                total_difficulty: U256::from(58_750_000_000_000_000_000_000_u128),
                timestamp: now,
            }
        }
    };

    // Create runtime from existing tokio handle
    let runtime = reth_tasks::RuntimeBuilder::new(reth_tasks::RuntimeConfig::default()
        .with_tokio(reth_tasks::runtime::TokioConfig::existing_handle(tokio::runtime::Handle::current())))
        .build()?;

    let mut net_builder = NetworkConfigBuilder::<EthNetworkPrimitives>::new(secret_key, runtime)
        .listener_port(net_config.p2p_port)
        .discovery_port(net_config.discovery_port)
        .peer_config(peers_config)
        .set_head(head)
        .block_import(Box::new(block_import))
        .disable_dns_discovery();

    // Parse custom bootnodes from config
    let custom_bootnodes: Vec<reth_network_peers::NodeRecord> = net_config
        .bootnodes
        .iter()
        .filter_map(|e| e.parse().ok())
        .collect();

    let chain_spec = if is_polygon {
        let polygon_spec = polygon::polygon_chain_spec();
        let mut bootnodes = polygon::polygon_bootnodes();
        bootnodes.extend(custom_bootnodes);
        info!(bootnodes = bootnodes.len(), "using Polygon bootnodes");
        net_builder = net_builder.boot_nodes(bootnodes);
        polygon_spec
    } else {
        if !custom_bootnodes.is_empty() {
            net_builder = net_builder.boot_nodes(custom_bootnodes);
        } else {
            net_builder = net_builder.mainnet_boot_nodes();
        }
        MAINNET.clone()
    };

    let network_config = net_builder.build_with_noop_provider(chain_spec);

    // Build the network
    let mut network = NetworkManager::new(network_config).await?;

    // Wire up the ETH request handler channel
    let (eth_req_tx, eth_req_rx) = mpsc::channel(256);
    network.set_eth_request_handler(eth_req_tx);

    // Build with transactions manager
    let builder = network
        .into_builder()
        .transactions(tx_pool.clone(), Default::default());

    let (network_handle, network, transactions, _) = builder.split_with_handle();

    info!(
        peer_id = %network_handle.peer_id(),
        "sentry node started, listening for peers"
    );

    // Spawn network manager and transaction manager as background tasks
    tokio::spawn(network);
    tokio::spawn(transactions);

    // Get FetchClient for proxying block requests to internet peers
    let fetch_client = network_handle.fetch_client().await?;

    // Spawn ETH request handler (cache + proxy fallback)
    tokio::spawn(eth_proxy::start_eth_request_handler(
        eth_req_rx,
        block_cache.clone(),
        fetch_client,
    ));

    // Spawn NewBlock rebroadcast task
    let rebroadcast_handle = network_handle.clone();
    let shutdown_rebroadcast = shutdown.clone();
    tokio::spawn(rebroadcast_new_blocks(
        rebroadcast_rx,
        rebroadcast_handle,
        shutdown_rebroadcast,
    ));

    // Subscribe to network events
    let mut events = network_handle.event_listener();

    // Spawn a task to monitor peer connections
    let handle_clone = network_handle.clone();
    let shutdown_monitor = shutdown.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let num_peers = handle_clone.num_connected_peers();
                    info!(num_peers, "peer count update");
                }
                _ = shutdown_monitor.cancelled() => break,
            }
        }
    });

    // Spawn a task to listen for new transactions in the pool and forward them
    let pool_clone = tx_pool.clone();
    let shutdown_tx = shutdown.clone();
    tokio::spawn(async move {
        let mut pending_txs = pool_clone.pending_transactions_listener();
        info!("listening for new pending transactions to forward");

        loop {
            tokio::select! {
                Some(tx_hash) = pending_txs.recv() => {
                    debug!(%tx_hash, "new pending transaction detected");

                    if let Some(tx) = pool_clone.get(&tx_hash) {
                        let consensus_tx = tx.transaction.clone_into_consensus();
                        let raw_tx = consensus_tx.inner().encoded_2718();

                        forwarder
                            .forward(ForwardableTx {
                                hash: tx_hash,
                                raw_tx,
                            })
                            .await;
                    }
                }
                _ = shutdown_tx.cancelled() => break,
            }
        }
    });

    // Main event loop - handle network events until shutdown
    loop {
        tokio::select! {
            Some(event) = events.next() => {
                match event {
                    NetworkEvent::ActivePeerSession { info, .. } => {
                        info!(
                            peer_id = %info.peer_id,
                            remote_addr = %info.remote_addr,
                            client_version = %info.client_version,
                            "new peer connected"
                        );
                    }
                    NetworkEvent::Peer(peer_event) => match peer_event {
                        PeerEvent::SessionClosed { peer_id, reason } => {
                            debug!(
                                %peer_id,
                                ?reason,
                                "peer disconnected"
                            );
                        }
                        PeerEvent::PeerAdded(peer_id) => {
                            debug!(%peer_id, "peer added to pool");
                        }
                        PeerEvent::PeerRemoved(peer_id) => {
                            debug!(%peer_id, "peer removed from pool");
                        }
                        PeerEvent::SessionEstablished(_) => {}
                    },
                }
            }
            _ = shutdown.cancelled() => {
                info!("shutdown signal received, stopping network");
                break;
            }
        }
    }

    // Graceful shutdown: save block cache, then disconnect peers
    if let Err(e) = block_cache.save_to_file(&cache_path) {
        warn!("failed to save block cache: {}", e);
    }
    network_handle.shutdown().await?;
    info!("network stopped");

    Ok(())
}

/// Rebroadcast received NewBlock messages to all connected peers
/// and update the network status to reflect the latest head.
async fn rebroadcast_new_blocks(
    mut rx: mpsc::UnboundedReceiver<crate::block_import::NewBlockData>,
    handle: NetworkHandle<EthNetworkPrimitives>,
    shutdown: CancellationToken,
) {
    info!("NewBlock rebroadcast task started");

    loop {
        tokio::select! {
            Some(new_block) = rx.recv() => {
                let header = &new_block.block.block.header;
                let block_number = header.number;

                // Update network status so our fork ID stays current
                handle.update_status(Head {
                    number: block_number,
                    hash: new_block.hash,
                    difficulty: U256::ZERO,
                    total_difficulty: U256::ZERO,
                    timestamp: header.timestamp,
                });

                // Get all connected peers and send the NewBlock to each
                let peers = match handle.get_all_peers().await {
                    Ok(peers) => peers,
                    Err(e) => {
                        warn!(error = %e, "failed to get peer list for rebroadcast");
                        continue;
                    }
                };

                let peer_count = peers.len();
                for peer in peers {
                    let msg = PeerMessage::NewBlock(NewBlockMessage {
                        hash: new_block.hash,
                        block: new_block.block.clone(),
                    });
                    handle.send_eth_message(peer.remote_id, msg);
                }

                debug!(
                    block_number,
                    hash = %new_block.hash,
                    peer_count,
                    "rebroadcast NewBlock to peers"
                );
            }
            _ = shutdown.cancelled() => break,
        }
    }

    info!("NewBlock rebroadcast task stopped");
}
