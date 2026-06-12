use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use reqwest::Client;
use serde_json::Value;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

const NODES: [&str; 5] = [
    "https://mainnet.helius-rpc.com/?api-key=ffe3568c-a4ff-4b2f-a2f5-53d891278489", // helius
    "https://solana-mainnet.g.alchemy.com/v2/Lrz0o5fwZvNv7XHB5OGNn",              // alchemy
    "https://tiniest-weathered-hexagon.solana-mainnet.quiknode.pro/cbbc61af5732a8a4e3d8ef395c0e79b580496022/", // quicknode
    "https://rpc.ankr.com/solana_devnet/2f5a4dc8e43c3446315186397fd4f9200c32075b310dd7e31acad693f5938dd8",  // ankr
    "https://api.mainnet-beta.solana.com",                                        // mainnet-beta
];

// Define our shared application state
struct AppState {
    current_node_index: AtomicUsize,
    http_client: Client,
}

#[tokio::main]
async fn main() {
    // Initialize shared state
    let shared_state = Arc::new(AppState {
        current_node_index: AtomicUsize::new(0),
        http_client: Client::new(), // Reusing the client is much faster
    });

    let app = Router::new()
        .route("/", get(|| async { "Hello World" }))
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