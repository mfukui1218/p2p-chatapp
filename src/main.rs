mod message;
mod network;
mod storage;

use std::error::Error;
use std::time::Duration;

use futures::stream::StreamExt;
use libp2p::{gossipsub, mdns, swarm::SwarmEvent, Multiaddr};
use tokio::{io, io::AsyncBufReadExt, select};

use message::Message;
use network::{Board, BoardEvent};
use storage::Store;

const DEFAULT_THREAD: &str = "general";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    let name = arg_val(&args, "--name").unwrap_or_else(|| "node".to_string());
    let auto = arg_val(&args, "--auto");
    let dial_peer = arg_val(&args, "--peer");

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

    let past = store.thread_messages(DEFAULT_THREAD);
    if past.is_empty() {
        println!("[{name}] (過去ログなし)");
    } else {
        println!("[{name}] === 過去ログ復元 {}件 ===", past.len());
        for m in &past {
            println!("    {} ({}): {}", m.sender_name, m.timestamp, m.text);
        }
        println!("[{name}] =====================");
    }

    let mut stdin = io::BufReader::new(io::stdin()).lines();

    let mut auto_timer = tokio::time::interval(Duration::from_secs(5));
    auto_timer.tick().await;
    let mut auto_sent = false;

    loop {
        select! {
            Ok(Some(line)) = stdin.next_line() => {
                if line.trim() == "/log" {
                    print_log(&store, &name);
                } else {
                    let msg = Message::new(DEFAULT_THREAD, &my_id, &name, &line);
                    store.insert(&msg);
                    publish(&mut swarm, &topic, &name, &msg);
                }
            }
            _ = auto_timer.tick() => {
                if let (Some(text), false) = (&auto, auto_sent) {
                    let msg = Message::new(DEFAULT_THREAD, &my_id, &name, text);
                    store.insert(&msg);
                    publish(&mut swarm, &topic, &name, &msg);
                    println!("[{name}] >> sent auto message");
                    auto_sent = true;
                }
            }
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
                            println!(
                                "[{name}] GOT [{}] {} ({}): {}  (stored={})",
                                msg.thread_id, msg.sender_name, msg.timestamp,
                                msg.text, store.len()
                            );
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

fn print_log(store: &Store, name: &str) {
    let msgs = store.thread_messages(DEFAULT_THREAD);
    println!("[{name}] --- {} ({}件) ---", DEFAULT_THREAD, msgs.len());
    for m in &msgs {
        println!("    {} ({}): {}", m.sender_name, m.timestamp, m.text);
    }
    println!("[{name}] ------------------");
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

fn arg_val(args: &[String], key: &str) -> Option<String> {
    args.iter()
        .position(|a| a == key)
        .and_then(|i| args.get(i + 1).cloned())
}