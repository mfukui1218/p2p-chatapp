import { useState, useEffect } from "react";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import "./App.css";

// Rust の Message と同じ形（受信したデータの型）
type Message = {
  id: string;
  thread_id: string;
  sender: string;
  sender_name: string;
  text: string;
  timestamp: number;
};

function App() {
  const [text, setText] = useState("");          // 入力中の投稿
  const [messages, setMessages] = useState<Message[]>([]);  // 受信した全メッセージ

  // 起動時に1回：Rustからの "new_post" を待ち受ける
  useEffect(() => {
    const unlisten = listen<Message>("new_post", (event) => {
      setMessages((prev) => [...prev, event.payload]);  // 届いたら一覧に追加
    });
    return () => {
      unlisten.then((f) => f());  // 後片付け
    };
  }, []);

  // 投稿ボタン：send_message コマンドを呼ぶ
  async function send() {
    if (text.trim() === "") return;
    await invoke("send_message", { text });
    setText("");  // 入力欄をクリア
  }

  return (
    <main className="container">
      <h1>P2P掲示板</h1>

      <div style={{ textAlign: "left", margin: "1rem 0", minHeight: "200px" }}>
        {messages.map((m) => (
          <div key={m.id} style={{ padding: "4px 0", borderBottom: "1px solid #eee" }}>
            <strong>{m.sender_name}</strong>: {m.text}
          </div>
        ))}
      </div>

      <form
        className="row"
        onSubmit={(e) => {
          e.preventDefault();
          send();
        }}
      >
        <input
          value={text}
          onChange={(e) => setText(e.currentTarget.value)}
          placeholder="メッセージを入力..."
        />
        <button type="submit">投稿</button>
      </form>
    </main>
  );
}

export default App;
