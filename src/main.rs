use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;
use warp::Filter;

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
    #[serde(skip_serializing_if = "Option::is_none")]
    label: Option<String>,
}

type PeerMap = Arc<DashMap<String, mpsc::UnboundedSender<PeerMessage>>>;

#[tokio::main]
async fn main() {
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(10000);

    let peers: PeerMap = Arc::new(DashMap::new());

    // GET /peerjs/id
    let peers_id = peers.clone();
    let id_route = warp::path!("peerjs" / "id")
        .and(warp::get())
        .map(move || {
            let _ = &peers_id;
            warp::reply::json(&serde_json::json!({
                "id": Uuid::new_v4().to_string()
            }))
        });

    // GET /peerjs/peers
    let peers_list = peers.clone();
    let list_route = warp::path!("peerjs" / "peers")
        .and(warp::get())
        .map(move || {
            let ids: Vec<String> = peers_list.iter().map(|e| e.key().clone()).collect();
            warp::reply::json(&ids)
        });

    // WebSocket /peerjs
    let peers_ws = peers.clone();
    let ws_route = warp::path("peerjs")
        .and(warp::ws())
        .and(warp::query::<std::collections::HashMap<String, String>>())
        .map(move |ws: warp::ws::Ws, query: std::collections::HashMap<String, String>| {
            let peers = peers_ws.clone();
            ws.on_upgrade(move |socket| handle_ws(socket, peers, query))
        });

    let routes = id_route.or(list_route).or(ws_route);

    println!("PeerJS server started on port {}", port);
    warp::serve(routes).run(([0, 0, 0, 0], port)).await;
}

async fn handle_ws(
    ws: warp::ws::WebSocket,
    peers: PeerMap,
    query: std::collections::HashMap<String, String>,
) {
    let peer_id = query.get("id").cloned().unwrap_or_else(|| Uuid::new_v4().to_string());

    // Проверка на дубликат
    if peers.contains_key(&peer_id) {
        let (mut tx, _rx) = ws.split();
        let err = PeerMessage {
            msg_type: "ID-TAKEN".to_string(),
            payload: Some(serde_json::json!({"msg": "ID is taken"})),
            src: None,
            dst: None,
            label: None,
        };
        let _ = tx.send(warp::ws::Message::text(serde_json::to_string(&err).unwrap())).await;
        return;
    }

    let (tx, mut rx) = mpsc::unbounded_channel::<PeerMessage>();
    peers.insert(peer_id.clone(), tx.clone());

    println!("Peer connected: {}", peer_id);

    let (mut ws_tx, mut ws_rx) = ws.split();

    // Отправка исходящих сообщений из канала в WebSocket
    let send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if let Ok(txt) = serde_json::to_string(&msg) {
                if ws_tx.send(warp::ws::Message::text(txt)).await.is_err() {
                    break;
                }
            }
        }
    });

    // Чтение сообщений из WebSocket
    while let Some(result) = ws_rx.next().await {
        match result {
            Ok(msg) => {
                if msg.is_text() {
                    if let Ok(text) = msg.to_str() {
                        if let Ok(mut peer_msg) = serde_json::from_str::<PeerMessage>(text) {
                            peer_msg.src = Some(peer_id.clone());

                            // Отправка конкретному получателю
                            if let Some(dst) = &peer_msg.dst {
                                if let Some(target) = peers.get(dst) {
                                    let _ = target.send(peer_msg.clone());
                                }
                            }
                            // Broadcast для LEAVE
                            else if peer_msg.msg_type == "LEAVE" {
                                for entry in peers.iter() {
                                    if entry.key() != &peer_id {
                                        let _ = entry.value().send(peer_msg.clone());
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Err(_) => break,
        }
    }

    peers.remove(&peer_id);
    send_task.abort();
    println!("Peer disconnected: {}", peer_id);
}
