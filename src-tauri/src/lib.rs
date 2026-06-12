mod p2p;

use tauri::State;
use tokio::sync::mpsc::Sender;
use p2p::event_loop::{Command, run_event_loop};

#[tauri::command]
fn greet(name: &str) -> String {
    format!("Hello, {}! You've been greeted from Rust!", name)
}

// ⑤ 画面の投稿を受けて、event_loopへ流すコマンド
#[tauri::command]
async fn send_message(
    text: String,
    tx: State<'_, Sender<Command>>,
) -> Result<(), String> {
    tx.send(Command::Send { text })
        .await
        .map_err(|e| e.to_string())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    // ① チャネルを作る（tx=入口、rx=出口）
    let (tx, rx) = tokio::sync::mpsc::channel::<Command>(100);

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .manage(tx)                          // ③ txを保管庫へ
        .setup(move |app| {                  // ④ event_loopを裏で起動
            let handle = app.handle().clone();
            tokio::spawn(run_event_loop(
                handle,
                rx,
                std::env::var("NODE_NAME").unwrap_or_else(|_| "node1".to_string()),
                None,
            ));
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![greet, send_message])  // ⑤ 登録
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}