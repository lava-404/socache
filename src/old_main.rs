use axum:: {
  Error, Json, Router, routing::{get,post}
};

use reqwest::{Client, Response};

use serde_json::Value;

const NODES: [&str; 5] = ["https://mainnet.helius-rpc.com/?api-key=ffe3568c-a4ff-4b2f-a2f5-53d891278489", //helius
                             "https://solana-mainnet.g.alchemy.com/v2/Lrz0o5fwZvNv7XHB5OGNn",  // alchemy
                             "https://tiniest-weathered-hexagon.solana-mainnet.quiknode.pro/cbbc61af5732a8a4e3d8ef395c0e79b580496022/", // quicknode
                             "https://rpc.ankr.com/solana_devnet/2f5a4dc8e43c3446315186397fd4f9200c32075b310dd7e31acad693f5938dd8",//ankr
                             "https://api.mainnet-beta.solana.com"];

#[tokio::main]
async fn main() {
  let app = Router::new().route("/", get(|| async {"Hello World"})).route("/", post(handle_post_with_alchemy));
  let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
  axum::serve(listener, app).await.unwrap();
}

async fn handle_post_with_alchemy (Json(payload): Json<Value>) -> Json<Value> {
  let alchemy_api_key = "Lrz0o5fwZvNv7XHB5OGNn";
  let url = format!("https://solana-mainnet.g.alchemy.com/v2/{}", alchemy_api_key);
  let client = Client::new();
  let response = client
  .post(&url)
  .header("Content-Type", "application/json").json(&payload).send().await.unwrap();

  let body: Value = response.json().await.unwrap();
  Json(body)

}

async fn handle_post_with_async(
  Json(payload): Json<Value>
) -> Option<Json<Value>> {

  for (i, url) in NODES.iter().enumerate() {
      let client = Client::new();

      let response = client
          .post(*url)
          .json(&payload)
          .send()
          .await;

      if let Ok(response) = response {
          let body: Value = response.json().await.ok()?;

          return Some(Json(body));
      }
  }

  None
}

