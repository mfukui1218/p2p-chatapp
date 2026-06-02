// P2P掲示板 / Week1 心臓部
// mDNSでピア発見 → gossipsubで全ノードへ中継。UIなし・CLIのみ。
// stdinに打った行が、中央サーバーを介さず他の全ノードに飛ぶ。
//
// 使い方:
//   cargo run -- --name node1
//   cargo run -- --name node2
//   cargo run -- --name node3
//   （別ターミナルで3つ起動 → mDNSで勝手に発見しあう → どれかに打つと全員に出る）
//
// テスト用に1回だけ自動送信:
//   cargo run -- --name node2 --auto "hello from node2"
//
// 撤退ライン(設計メモ#6): mDNS発見がコケる環境(WSL2等)では明示dialに切替。
//   node1の listening 行に出る multiaddr を控えて:
//   cargo run -- --name node2 --peer /ip4/127.0.0.1/tcp/<port>/p2p/<peerid>
use std::error::Error;
use std::time::Duration;

use futures::stream::StreamExt;
use libp2p::{
    gossipsub, mdns, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux, Multiaddr,
};
use tokio::{io, io::AsyncBufReadExt, select};

#[derive(NetworkBehaviour)]
struct Board {
    gossipsub: gossipsub::Behaviour,
    mdns: mdns::tokio::Behaviour,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // --- 雑なCLI引数パース（UIは作り込まない方針） ---
    let args: Vec<String> = std::env::args().collect();
    let name = arg_val(&args, "--name").unwrap_or_else(|| "node".to_string());
    let auto = arg_val(&args, "--auto");
    let dial_peer = arg_val(&args, "--peer");

    let mut swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_behaviour(|key| {
            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .heartbeat_interval(Duration::from_secs(1))
                .validation_mode(gossipsub::ValidationMode::Strict)
                .build()?;

            let gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub_config,
            )?;

            let mdns =
                mdns::tokio::Behaviour::new(mdns::Config::default(), key.public().to_peer_id())?;

            Ok(Board { gossipsub, mdns })
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    // 全ノードが同じトピックをsubscribeする。これが「同じ掲示板」の単位。
    let topic = gossipsub::IdentTopic::new("p2p-board");
    swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

    swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    // 撤退ライン用: 明示dial（mDNSが効かない環境のフォールバック）
    if let Some(addr) = &dial_peer {
        let ma: Multiaddr = addr.parse()?;
        swarm.dial(ma)?;
        println!("[{name}] dialing {addr}");
    }

    println!("[{name}] peer_id = {}", swarm.local_peer_id());

    let mut stdin = io::BufReader::new(io::stdin()).lines();

    // テスト用オートメッセージ: 起動5秒後に1回だけ送る
    let mut auto_timer = tokio::time::interval(Duration::from_secs(5));
    auto_timer.tick().await; // 最初の即時tickは捨てる
    let mut auto_sent = false;

    loop {
        select! {
            // stdin → publish（手で打つ通常モード）
            Ok(Some(line)) = stdin.next_line() => {
                publish(&mut swarm, &topic, &name, &line);
            }
            // テスト用の自動送信
            _ = auto_timer.tick() => {
                if let (Some(msg), false) = (&auto, auto_sent) {
                    publish(&mut swarm, &topic, &name, msg);
                    println!("[{name}] >> sent auto message");
                    auto_sent = true;
                }
            }
            // libp2pイベントループ
            event = swarm.select_next_some() => match event {
                // mDNSで新ピア発見 → gossipsubのメッシュに入れる（#21の本体）
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
                // 明示dialで繋がった相手もメッシュに入れる（撤退ライン経路）
                SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                    println!("[{name}] connected: {peer_id}");
                    swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                }
                // メッセージ受信 → 表示（#25 中継が動いた証拠がここに出る）
                SwarmEvent::Behaviour(BoardEvent::Gossipsub(gossipsub::Event::Message {
                    propagation_source: src,
                    message,
                    ..
                })) => {
                    println!(
                        "[{name}] GOT: '{}' (relayed via {src})",
                        String::from_utf8_lossy(&message.data)
                    );
                }
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("[{name}] listening on {address}/p2p/{}", swarm.local_peer_id());
                }
                _ => {}
            }
        }
    }
}

fn publish(swarm: &mut libp2p::Swarm<Board>, topic: &gossipsub::IdentTopic, name: &str, body: &str) {
    let payload = format!("{name}: {body}");
    if let Err(e) = swarm
        .behaviour_mut()
        .gossipsub
        .publish(topic.clone(), payload.into_bytes())
    {
        // "InsufficientPeers" は「まだ誰も繋がってない」= 正常な初期状態
        println!("[{name}] publish error: {e:?}");
    }
}

fn arg_val(args: &[String], key: &str) -> Option<String> {
    args.iter()
        .position(|a| a == key)
        .and_then(|i| args.get(i + 1).cloned())
}
