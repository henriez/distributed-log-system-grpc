import sqlite3
from typing import Optional


class RaftDatabase:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()
        self._ensure_state_row()
        self.current_term, self.voted_for, self.commit_index = self._load_state()

    def _create_tables(self) -> None:
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS state (
                id INTEGER PRIMARY KEY,
                current_term INTEGER NOT NULL DEFAULT 0,
                voted_for TEXT,
                commit_index INTEGER NOT NULL DEFAULT 0
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                log_index INTEGER PRIMARY KEY,
                term INTEGER NOT NULL,
                data TEXT NOT NULL,
                hash TEXT NOT NULL
            )
        """)
        self.conn.commit()

    def _ensure_state_row(self) -> None:
        cur = self.conn.execute("SELECT COUNT(*) FROM state")
        if cur.fetchone()[0] == 0:
            self.conn.execute(
                "INSERT INTO state (id, current_term, voted_for, commit_index) VALUES (1, 0, NULL, 0)"
            )
            self.conn.commit()

    def _load_state(self):
        cur = self.conn.execute(
            "SELECT current_term, voted_for, commit_index FROM state WHERE id = 1"
        )
        row = cur.fetchone()
        return (row["current_term"], row["voted_for"], row["commit_index"]) if row else (0, None, 0)

    def get_state(self) -> dict:
        return {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "commit_index": self.commit_index,
        }

    def update_state(self, current_term: int, voted_for: Optional[str], commit_index: int) -> None:
        self.conn.execute(
            "UPDATE state SET current_term = ?, voted_for = ?, commit_index = ? WHERE id = 1",
            (current_term, voted_for, commit_index),
        )
        self.conn.commit()
        self.current_term = current_term
        self.voted_for = voted_for
        self.commit_index = commit_index

    def append_log(self, log_index: int, term: int, data: str, hash: str) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO logs (log_index, term, data, hash) VALUES (?, ?, ?, ?)",
            (log_index, term, data, hash),
        )
        self.conn.commit()

    def get_logs_from(self, start_index: int) -> list[dict]:
        cur = self.conn.execute(
            "SELECT log_index, term, data, hash FROM logs WHERE log_index >= ? ORDER BY log_index",
            (start_index,),
        )
        return [
            {"log_index": r["log_index"], "term": r["term"], "data": r["data"], "hash": r["hash"]}
            for r in cur.fetchall()
        ]

    def get_log_entry(self, log_index: int) -> Optional[dict]:
        cur = self.conn.execute(
            "SELECT log_index, term, data, hash FROM logs WHERE log_index = ?",
            (log_index,),
        )
        row = cur.fetchone()
        if row:
            return {"log_index": row["log_index"], "term": row["term"], "data": row["data"], "hash": row["hash"]}
        return None

    def delete_logs_from(self, start_index: int) -> None:
        self.conn.execute("DELETE FROM logs WHERE log_index >= ?", (start_index,))
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()
