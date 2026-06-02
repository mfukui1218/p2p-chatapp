use crate::message::Message;
use rusqlite::{params, Connection};

pub struct Store {
    conn: Connection,
}

impl Store {
    pub fn open(node_name: &str) -> rusqlite::Result<Self> {
        let path = format!("data/{node_name}.db");
        let conn = Connection::open(path)?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS messages (
                id          TEXT PRIMARY KEY,
                thread_id   TEXT NOT NULL,
                sender      TEXT NOT NULL,
                sender_name TEXT NOT NULL,
                text        TEXT NOT NULL,
                timestamp   INTEGER NOT NULL
            )",
            [],
        )?;
        Ok(Store { conn })
    }

    pub fn insert(&mut self, msg: &Message) {
        let r = self.conn.execute(
            "INSERT OR IGNORE INTO messages
                (id, thread_id, sender, sender_name, text, timestamp)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                msg.id, msg.thread_id, msg.sender,
                msg.sender_name, msg.text, msg.timestamp
            ],
        );
        if let Err(e) = r {
            eprintln!("[storage] insert error: {e:?}");
        }
    }

    pub fn thread_messages(&self, thread_id: &str) -> Vec<Message> {
        let mut out = Vec::new();
        let mut stmt = match self.conn.prepare(
            "SELECT id, thread_id, sender, sender_name, text, timestamp
             FROM messages WHERE thread_id = ?1 ORDER BY timestamp ASC",
        ) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[storage] query error: {e:?}");
                return out;
            }
        };
        let rows = stmt.query_map(params![thread_id], |row| {
            Ok(Message {
                id: row.get(0)?,
                thread_id: row.get(1)?,
                sender: row.get(2)?,
                sender_name: row.get(3)?,
                text: row.get(4)?,
                timestamp: row.get(5)?,
            })
        });
        if let Ok(rows) = rows {
            for m in rows.flatten() {
                out.push(m);
            }
        }
        out
    }

    pub fn len(&self) -> usize {
        self.conn
            .query_row("SELECT COUNT(*) FROM messages", [], |r| r.get::<_, i64>(0))
            .unwrap_or(0) as usize
    }
}