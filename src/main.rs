use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use axum::extract::ws::{
    WebSocket,
    WebSocketUpgrade,
};
use axum::response::Response;
use tokio::sync::RwLock;
use reqwest::Client;
use serde_json::Value;
use std::{collections::{HashSet, HashMap},sync::{
    Arc, atomic::{AtomicUsize, Ordering}
}};
use tokio::sync::mpsc;
use futures_util::{SinkExt, lock::Mutex, stream::{SplitSink}};   
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
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
const WS_URL: &str= "wss://devnet.helius-rpc.com/?api-key=ffe3568c-a4ff-4b2f-a2f5-53d891278489";
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
    pending_requests: RwLock<HashMap<u64, String>>,

    active_subscriptions: RwLock<HashMap<u64, String>>,
    clients: RwLock<
    HashMap<ClientId, mpsc::Sender<axum::extract::ws::Message>
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
        socket_write: Mutex::new(write),
        pending_requests: RwLock::new(HashMap::new()),
        active_subscriptions: RwLock::new(HashMap::new()),
        clients: RwLock::new(HashMap::new())

    });
    let helius_state = shared_state.clone();

    tokio::spawn(async move {
        upstream_reader_loop(read, helius_state).await;
    });

    let app = Router::new()
        .route("/", get(|| async { "Hello World" }))
        .route("/ws", get(ws_handler))
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

async fn handle_subscription(payload: Value, state: Arc<AppState>, client_id: Uuid ) {
    let method = payload["method"].as_str().unwrap_or("");
    if method == "accountSubscribe" {
        let acc: Option<&str> = payload
        .get("params")
        .and_then(|p| p.get(0))
        .and_then(|m| m.as_str()); // Use map to convert &str to String

        let acc_id = match acc {
            Some(acc) => acc,
            None => return,
        };
        // Now acc is Option<String>
        let needs_subscribe = !state.subscriptions.read().await.contains_key(acc_id);

        if needs_subscribe {
            let rpc_id = rand::random::<u64>();
            let mut outgoing = payload.clone();
            outgoing["id"] = serde_json::json!(rpc_id);
            state
            .pending_requests
            .write()
            .await
            .insert(rpc_id, acc_id.to_string());
            let mut writer = state.socket_write.lock().await;
            writer.send(Message::text((outgoing.to_string()))).await.unwrap();
        }

        let mut subs = state.subscriptions.write().await;
        subs.entry(acc_id.to_string()).or_insert_with(HashSet::new).insert(client_id);

    }
}

async fn upstream_reader_loop(
    mut ws_stream: futures_util::stream::SplitStream<
        WebSocketStream<MaybeTlsStream<TcpStream>>
    >,
    state: Arc<AppState>,
){
    while let Some(Ok(msg)) = ws_stream.next().await {
        if let Message::Text((text)) = msg {
            if let Ok(json) = serde_json::from_str::<Value>(&text) {
                if let Some(sub_id) = json.get("result").and_then(|r| r.as_u64()) {
                    if json.get("method").is_none() {
                        if let Some(req_id) = json.get("id").and_then(|i| i.as_u64()){

                            if let Some(account) = state.pending_requests.write().await.remove(&req_id){
                                state
                                .active_subscriptions
                                .write()
                                .await
                                .insert(sub_id, account);
                            }
                    
                        
                        }
                    }
                }

                if let Some(method) = json.get("method").and_then(|i| i.as_str()){
                    if method == "accountNotification" {
                        println!("Got notification from helius");
                        if let Some(sub_id) = json["params"]["subscription"].as_u64() {
                            if let Some(acc) = state.active_subscriptions.read().await.get(&sub_id) {
                                if let Some(client_ids) =
                                state.subscriptions.read().await.get(acc)
                            {
                                for client_id in client_ids {
                                    if let Some(sender) =
                                        state.clients.read().await.get(client_id)
                                    {
                                        let _ = sender
                                        .send(axum::extract::ws::Message::Text(
                                            text.to_string().into()
                                        ))
                                            .await;
                                        println!("Sending notification to client {}", client_id);
                                    }
                                }
                            }
                            }
                           

                        }
                    }
                }
            }
        }
    }
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> Response {
    ws.on_upgrade(move |socket| {
        handle_client(socket, state)
    })
}

async fn handle_client(
    socket: WebSocket,
    state: Arc<AppState>,
) {
    let client_id = Uuid::new_v4();

    let (tx, mut rx) =
        mpsc::channel::<axum::extract::ws::Message>(100);

    let (mut sender, mut receiver) = socket.split();

    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if let Err(e) = sender.send(msg).await {
                println!("Send failed: {:?}", e);
                break;
            }
        }
    });

    state
        .clients
        .write()
        .await
        .insert(client_id, tx);

    while let Some(Ok(msg)) = receiver.next().await {
        if let axum::extract::ws::Message::Text(text) = msg {
            if let Ok(payload) =
                serde_json::from_str::<Value>(&text)
            {
                handle_subscription(
                    payload,
                    state.clone(),
                    client_id,
                    
                )
                .await;
            }
        }
    }
    state
    .clients
    .write()
    .await
    .remove(&client_id);

    let mut subs = state.subscriptions.write().await;

    for clients in subs.values_mut() {
        clients.remove(&client_id);
    }
    subs.retain(|_, clients| !clients.is_empty());

    println!("Client disconnected: {}", client_id);
}