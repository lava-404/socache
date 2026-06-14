use axum::{
  extract::State,
  http::StatusCode,
  routing::{get, post},
  Json, Router,
};
use tokio::sync::RwLock;
use reqwest::Client;
use serde_json::Value;
use std::{collections::{HashSet, HashMap},sync::{
  Arc, atomic::{AtomicUsize, Ordering}
}};
use futures_util::SinkExt;   
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Utf8Bytes};
use tokio::net::TcpStream;
use moka::future::Cache;
use std::time::Duration;
use futures_util::{StreamExt};
use tokio_tungstenite::{connect_async};
use tokio_tungstenite::tungstenite::protocol::{Message};
use uuid::Uuid;
type ClientId = Uuid;
type Account = String;
type Subscriptions = HashMap<Account, HashSet<ClientId>>;
const NODES: [&str; 5] = [
  "https://mainnet.helius-rpc.com/?api-key=ffe3568c-a4ff-4b2f-a2f5-53d891278489", // helius
  "https://solana-mainnet.g.alchemy.com/v2/Lrz0o5fwZvNv7XHB5OGNn",              // alchemy
  "https://tiniest-weathered-hexagon.solana-mainnet.quiknode.pro/cbbc61af5732a8a4e3d8ef395c0e79b580496022/", // quicknode
  "https://rpc.ankr.com/solana_devnet/2f5a4dc8e43c3446315186397fd4f9200c32075b310dd7e31acad693f5938dd8",  // ankr
  "https://api.mainnet-beta.solana.com",                                        // mainnet-beta
];
const WS_URL: &str= "wss://mainnet.helius-rpc.com/?api-key=ffe3568c-a4ff-4b2f-a2f5-53d891278489";
const READ_METHODS: [&str; 43] = [
  // Cluster and Network Information
  "getClusterNodes",
  "getEpochInfo",
  "getEpochSchedule",
  "getGenesisHash",
  "getHealth",
  "getHighestSnapshotSlot",
  "getIdentity",
  "getLeaderSchedule",
  "getMaxRetransmitSlot",
  "getMaxShredInsertSlot",
  "getSlot",
  "getSlotLeader",
  "getSlotLeaders",
  "getVersion",
  "getVoteAccounts",

  // Blockchain State and Blocks
  "getBlock",
  "getBlocks",
  "getBlockHeight",
  "getBlockTime",
  "getBlockCommitment",
  "getFirstAvailableBlock",
  "getLatestBlockhash",
  "isBlockhashValid",

  // Account and Token Data
  "getAccountInfo",
  "getBalance",
  "getMultipleAccounts",
  "getTokenAccountsByDelegate",
  "getTokenAccountsByOwner",
  "getTokenLargestAccounts",
  "getStakeActivation",

  // Transaction Queries and History
  "getSignaturesForAddress",
  "getSignatureStatuses",
  "getTransaction",
  "getTransactionCount",
  "getRecentPerformanceSamples",

  // Economic and Fee Data
  "getInflationGovernor",
  "getInflationRate",
  "getInflationReward",
  "getStakeMinimumDelegation",
  "getSupply",
  "getFeeForMessage",
  "getRecentPrioritizationFees",
  "getMinimumBalanceForRentExemption",
];

// Define our shared application state
struct AppState {
  current_node_index: AtomicUsize,
  http_client: Client,
  cache: Cache<String, Value>,
  subscriptions: RwLock<HashMap<String, HashSet<Uuid>>>,
  socket_write: Mutex<
  SplitSink<
      WebSocketStream<MaybeTlsStream<TcpStream>>,
      Message
      >
  >,
}

#[tokio::main]
async fn main() {
  let (socket, _) = connect_async(WS_URL).await.unwrap();
  let (write, read) = socket.split();
  // Initialize shared state
  let shared_state = Arc::new(AppState {
      current_node_index: AtomicUsize::new(0),
      http_client: Client::new(), // Reusing the client is much faster
      cache: Cache::builder().time_to_live(Duration::from_secs(2)).build(),
      subscriptions: RwLock::new(HashMap::new()),
      socket: socket
  });

  let app = Router::new()
      .route("/", get(|| async { "Hello World" }))
      .route("/ws", post(handle_post_with_round_robin))
      .route("/", post(handle_post_with_round_robin))
      .with_state(shared_state); // Inject state into Axum

  println!("Server running on http://0.0.0.0:3000");
  let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
  axum::serve(listener, app).await.unwrap();
}

async fn handle_post_with_round_robin(
  State(state): State<Arc<AppState>>,
  Json(payload): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
  let num_nodes = NODES.len();
  // Atomically get the current index and increment it for the next request
  // Ordering::Relaxed is perfectly fine here since we just need a counter
  let start_index = state.current_node_index.fetch_add(1, Ordering::Relaxed) % num_nodes;

  let method = payload["method"]
  .as_str()
  .unwrap_or("");

  let is_read = READ_METHODS.contains(&method);
  let cache_key = format!(
      "{}:{}",
      payload["method"],
      payload["params"]
  );

  let hash = blake3::hash(cache_key.as_bytes());

  let key = hash.to_hex().to_string();

   
  if is_read {
      if let Some(cached) = state.cache.get(&key).await {
          println!("Cache hit");
          return Ok(Json(cached));
      }
  }
  
  // Loop through nodes starting from 'start_index', trying each one up to 'num_nodes' times
  for offset in 0..num_nodes {
      let current_idx = (start_index + offset) % num_nodes;
      let url = NODES[current_idx];


   

      // Send the request using our shared client
      let response_result = state
          .http_client
          .post(url)
          .json(&payload)
          .send()
          .await;

      match response_result {
          Ok(response) => {
              let status = response.status();

              // If the request was successful, return the body immediately
              if status.is_success() {
                  
                   
                      if let Ok(body) = response.json::<Value>().await {
                          if is_read {
                              state.cache.insert(key.clone(), body.clone()).await;
                          }
                      
                          return Ok(Json(body));
                      }
                  
              } 
              // Catch 429 Too Many Requests OR 5xx Server Errors
              else if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
                  println!("Node {} failed with status {}, retrying next...", url, status);
                  continue; // Immediately try the next node in the loop
              } 
              // If it's a 400 Bad Request, the payload is likely bad. 
              // Don't retry, just return an error to the user.
              else {
                  println!("Node {} returned client error {}, aborting...", url, status);
                  return Err(status);
              }
          }
          Err(e) => {
              // Catch network-level errors (timeouts, DNS failures) and retry
              println!("Network error connecting to {}: {}", url, e);
              continue;
          }
      }
  }

  // If the loop finishes without returning, all nodes have failed.
  println!("All nodes failed. Exhausted retry pool.");
  Err(StatusCode::SERVICE_UNAVAILABLE)
}

async fn handle_ws(Json(payload): Json<Value>, State(state): State<Arc<AppState>> ) {
  let method = payload["method"].as_str().unwrap_or("");
  if method.contains("accountSubscribe") {
      let acc: Option<String> = payload
      .get("params")
      .and_then(|p| p.get(0))
      .and_then(|m| m.as_str())
      .map(|s| s.to_string()); // Use map to convert &str to String

      let client_id = Uuid::new_v4();
      // Now acc is Option<String>
      if let Some(account_id) = acc {
          // account_id is now a String
          let account: Account = account_id; // Assuming Account is a type alias for String
          let mut subs: tokio::sync::RwLockWriteGuard<'_, HashMap<String, HashSet<Uuid>>> = state.subscriptions.write().await;
          if !subs.contains_key(&account){
              let (mut write, mut read) = state.socket.split();
              match write.send(Message::text(payload.to_string())).await {
                  Ok(_) => { /* Success */ }
                  Err(e) => eprintln!("Failed to send message: {}", e),
              }
          


          }
          subs.entry(account).or_insert_with(HashSet::new).insert(client_id);
      }
  
  }
}