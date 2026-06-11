use std::error::Error;

use futures::stream::StreamExt;
use libp2p::{gossipsub, mdns, swarm::SwarmEvent, Multiaddr};
use tauri::{AppHandle, Emitter};
use tokio::select;
use tokio::sync::mpsc;

use super::message::Message;
use super::network::{self, Board, BoardEvent};
use super::storage::Store;

const DEFAULT_THREAD: &str = "general";

pub enum Command {
    Send { text: String },
}

pub async fn run_event_loop(
    app: AppHandle,
    mut rx: mpsc::Receiver<Command>,
    name: String,
    dial_peer: Option<String>,
) -> Result<(), Box<dyn Error>> {
    let mut swarm = network::build_swarm()?;

    let topic = gossipsub::IdentTopic::new("p2p-board");
    swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

    swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    if let Some(addr) = &dial_peer {
        let ma: Multiaddr = addr.parse()?;
        swarm.dial(ma)?;
        println!("[{name}] dialing {addr}");
    }

    let my_id = swarm.local_peer_id().to_string();
    println!("[{name}] peer_id = {my_id}");

    let mut store = Store::open(&name)?;

    for m in store.thread_messages(DEFAULT_THREAD) {
        let _ = app.emit("new_post", &m);
    }

    loop {
        select! {
            Some(cmd) = rx.recv() => match cmd {
                Command::Send { text } => {
                    let msg = Message::new(DEFAULT_THREAD, &my_id, &name, &text);
                    store.insert(&msg);
                    let _ = app.emit("new_post", &msg);
                    publish(&mut swarm, &topic, &name, &msg);
                }
            },
            event = swarm.select_next_some() => match event {
                SwarmEvent::Behaviour(BoardEvent::Mdns(mdns::Event::Discovered(list))) => {
                    for (peer_id, _addr) in list {
                        println!("[{name}] mDNS discovered: {peer_id}");
                        swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                    }
                }
                SwarmEvent::Behaviour(BoardEvent::Mdns(mdns::Event::Expired(list))) => {
                    for (peer_id, _addr) in list {
                        println!("[{name}] mDNS expired: {peer_id}");
                        swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                    }
                }
                SwarmEvent::Behaviour(BoardEvent::Gossipsub(gossipsub::Event::Message {
                    message,
                    ..
                })) => {
                    match Message::from_bytes(&message.data) {
                        Ok(msg) => {
                            store.insert(&msg);
                            let _ = app.emit("new_post", &msg);
                        }
                        Err(e) => println!("[{name}] parse error: {e:?}"),
                    }
                }
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("[{name}] listening on {address}/p2p/{my_id}");
                }
                _ => {}
            }
        }
    }
}

fn publish(
    swarm: &mut libp2p::Swarm<Board>,
    topic: &gossipsub::IdentTopic,
    name: &str,
    msg: &Message,
) {
    let payload = match msg.to_bytes() {
        Ok(bytes) => bytes,
        Err(e) => {
            println!("[{name}] serialize error: {e:?}");
            return;
        }
    };
    if let Err(e) = swarm.behaviour_mut().gossipsub.publish(topic.clone(), payload) {
        println!("[{name}] publish error: {e:?}");
    }
}
