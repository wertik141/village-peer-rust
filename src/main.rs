use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use dashmap::DashMap;
use futures_util::{sink::SinkExt, stream::StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PeerMessage {
    #[serde(rename = "type")]
    msg_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    src: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dst: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload: Option<serde_json::Value>,
}

type PeerMap = Arc<DashMap<String, mpsc::UnboundedSender<PeerMessage>>>;

#[tokio::main]
async fn main() {
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(10000);

    let peers: PeerMap = Arc::new(DashMap::new());

    let app = Router::new()
        .route("/peerjs/id", get(get_id))
        .route("/peerjs/peers", get(get_peers))
        .route("/peerjs", get(ws_handler))
        .with_state(peers);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .expect("failed to bind");

    println!("PeerJS server started on port {}", port);
    axum::serve(listener, app).await.unwrap();
}

async fn get_id() -> impl IntoResponse {
    Json(serde_json::json!({ "id": Uuid::new_v4().to_string() }))
}

async fn get_peers(State(peers): State<PeerMap>) -> impl IntoResponse {
    let ids: Vec<String> = peers.iter().map(|e| e.key().clone()).collect();
    Json(ids)
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(query): Query<HashMap<String, String>>,
    State(peers): State<PeerMap>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, peers, query))
}

async fn handle_socket(
    socket: WebSocket,
    peers: PeerMap,
    query: HashMap<String, String>,
) {
    let peer_id = query
        .get("id")
        .cloned()
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    if peers.contains_key(&peer_id) {
        let (mut tx, _rx) = socket.split();
        let err = PeerMessage {
            msg_type: "ID-TAKEN".to_string(),
            payload: Some(serde_json::json!({ "msg": "ID is taken" })),
            src: None,
            dst: None,
        };
        let _ = tx
            .send(Message::Text(serde_json::to_string(&err).unwrap()))
            .await;
        return;
    }

    let (tx, mut rx) = mpsc::unbounded_channel::<PeerMessage>();
    peers.insert(peer_id.clone(), tx);
    println!("Peer connected: {}", peer_id);

    let (mut ws_tx, mut ws_rx) = socket.split();

    let send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if let Ok(txt) = serde_json::to_string(&msg) {
                if ws_tx.send(Message::Text(txt)).await.is_err() {
                    break;
                }
            }
        }
    });

    while let Some(Ok(msg)) = ws_rx.next().await {
        if let Message::Text(text) = msg {
            if let Ok(mut peer_msg) = serde_json::from_str::<PeerMessage>(&text) {
                peer_msg.src = Some(peer_id.clone());
                if let Some(dst) = &peer_msg.dst {
                    if let Some(target) = peers.get(dst) {
                        let _ = target.send(peer_msg.clone());
                    }
                } else if peer_msg.msg_type == "LEAVE" {
                    for entry in peers.iter() {
                        if entry.key() != &peer_id {
                            let _ = entry.value().send(peer_msg.clone());
                        }
                    }
                }
            }
        }
    }

    peers.remove(&peer_id);
    send_task.abort();
    println!("Peer disconnected: {}", peer_id);
}
