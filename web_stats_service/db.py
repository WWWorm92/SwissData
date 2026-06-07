import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional


class StatsDB:
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _conn(self):
        c = sqlite3.connect(str(self.db_path), timeout=15.0)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA busy_timeout=15000")
        c.execute("PRAGMA foreign_keys=ON")
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
        return c

    def _init_db(self):
        with self._conn() as c:
            c.executescript(
                """
                PRAGMA journal_mode=WAL;

                CREATE TABLE IF NOT EXISTS athletes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bib TEXT NOT NULL,
                    name TEXT NOT NULL,
                    category TEXT NOT NULL,
                    country TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL,
                    UNIQUE(category, bib)
                );

                CREATE TABLE IF NOT EXISTS runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_key TEXT NOT NULL,
                    category TEXT NOT NULL,
                    distance_m INTEGER NOT NULL,
                    run_started_at_text TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(run_key, category, distance_m, run_started_at_text)
                );

                CREATE TABLE IF NOT EXISTS results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    athlete_id INTEGER NOT NULL,
                    run_id INTEGER NOT NULL,
                    finish_sec REAL,
                    finish_text TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    raw_json TEXT NOT NULL,
                    FOREIGN KEY(athlete_id) REFERENCES athletes(id),
                    FOREIGN KEY(run_id) REFERENCES runs(id)
                );

                CREATE INDEX IF NOT EXISTS idx_ath_category_name ON athletes(category, name);
                CREATE INDEX IF NOT EXISTS idx_runs_category_distance ON runs(category, distance_m);
                CREATE INDEX IF NOT EXISTS idx_results_finish ON results(finish_sec);
                CREATE INDEX IF NOT EXISTS idx_results_athlete_created ON results(athlete_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_results_run_id ON results(run_id);

                CREATE TABLE IF NOT EXISTS splits (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    result_id INTEGER NOT NULL,
                    split_num INTEGER NOT NULL,
                    split_sec REAL,
                    split_text TEXT NOT NULL,
                    FOREIGN KEY(result_id) REFERENCES results(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_splits_result ON splits(result_id);
                """
            )

    def _with_retry(self, fn):
        tries = 3
        for i in range(tries):
            try:
                return fn()
            except sqlite3.OperationalError as e:
                txt = str(e).lower()
                if "locked" in txt and i < (tries - 1):
                    time.sleep(0.08 * (i + 1))
                    continue
                raise

    def upsert_result(self, p: Dict[str, Any]) -> bool:
        event_id = str(p.get("event_id") or "").strip()
        if not event_id:
            return False

        bib = str(p.get("bib") or "").strip()
        name = str(p.get("name") or "").strip() or bib
        category = str(p.get("category") or "").strip() or "Без категории"
        country = str(p.get("country") or "").strip().upper()
        run_key = str(p.get("run_key") or "").strip() or "?-?"
        distance_m = int(p.get("distance_m") or 0)
        run_started = str(p.get("run_started_at_text") or "").strip()
        created_at = str(p.get("created_at") or "")
        finish_text = str(p.get("finish_text") or "")
        status = str(p.get("status") or "")
        finish_sec = p.get("finish_sec")
        try:
            finish_sec = float(finish_sec) if finish_sec is not None else None
        except Exception:
            finish_sec = None

        def _tx() -> bool:
            with self._conn() as c:
                c.execute(
                    """
                    INSERT INTO athletes(bib, name, category, country, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(category, bib) DO UPDATE SET
                        name=excluded.name,
                        country=excluded.country,
                        updated_at=excluded.updated_at
                    """,
                    (bib, name, category, country, created_at),
                )

                arow = c.execute(
                    "SELECT id FROM athletes WHERE category=? AND bib=?",
                    (category, bib),
                ).fetchone()
                if not arow:
                    return False
                athlete_id = int(arow["id"])

                c.execute(
                    """
                    INSERT INTO runs(run_key, category, distance_m, run_started_at_text, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(run_key, category, distance_m, run_started_at_text) DO NOTHING
                    """,
                    (run_key, category, distance_m, run_started, created_at),
                )

                rrow = c.execute(
                    "SELECT id FROM runs WHERE run_key=? AND category=? AND distance_m=? AND run_started_at_text=?",
                    (run_key, category, distance_m, run_started),
                ).fetchone()
                if not rrow:
                    return False
                run_id = int(rrow["id"])

                c.execute(
                    """
                    INSERT INTO results(event_id, athlete_id, run_id, finish_sec, finish_text, status, created_at, raw_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(event_id) DO UPDATE SET
                        finish_sec=excluded.finish_sec,
                        finish_text=excluded.finish_text,
                        status=excluded.status,
                        created_at=excluded.created_at,
                        raw_json=excluded.raw_json
                    """,
                    (
                        event_id,
                        athlete_id,
                        run_id,
                        finish_sec,
                        finish_text,
                        status,
                        created_at,
                        json.dumps(p, ensure_ascii=False, separators=(",", ":")),
                    ),
                )

                rrow2 = c.execute(
                    "SELECT id FROM results WHERE event_id=?",
                    (event_id,),
                ).fetchone()
                if rrow2:
                    res_id = int(rrow2["id"])
                    c.execute("DELETE FROM splits WHERE result_id=?", (res_id,))
                    splits = p.get("splits") or {}
                    for k, v in sorted(splits.items(), key=lambda x: int(x[0]) if str(x[0]).isdigit() else 0):
                        try:
                            sn = int(k)
                        except Exception:
                            continue
                        st = str(v or "").strip()
                        try:
                            sv = float(st)
                        except Exception:
                            sv = None
                        c.execute(
                            "INSERT INTO splits(result_id, split_num, split_sec, split_text) VALUES (?, ?, ?, ?)",
                            (res_id, sn, sv, st),
                        )
            return True

        return bool(self._with_retry(_tx))

    def categories(self) -> List[str]:
        with self._conn() as c:
            rows = c.execute("SELECT DISTINCT category FROM athletes ORDER BY category").fetchall()
        return [str(r["category"]) for r in rows]

    def athletes(self, category: str) -> List[Dict[str, Any]]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT id, bib, name, country FROM athletes WHERE category=? ORDER BY name, bib",
                (category,),
            ).fetchall()
        return [dict(r) for r in rows]

    def distances(self, athlete_id: int) -> List[Dict[str, Any]]:
        with self._conn() as c:
            rows = c.execute(
                """
                SELECT r.distance_m,
                       MIN(res.finish_sec) AS best_sec,
                       COUNT(*) AS attempts
                FROM results res
                JOIN runs r ON r.id = res.run_id
                WHERE res.athlete_id=?
                  AND res.finish_sec IS NOT NULL
                GROUP BY r.distance_m
                ORDER BY r.distance_m
                """,
                (athlete_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def runs_for_distance(self, athlete_id: int, distance_m: int) -> List[Dict[str, Any]]:
        with self._conn() as c:
            rows = c.execute(
                """
                SELECT r.run_key,
                       r.run_started_at_text,
                       res.finish_sec,
                       res.finish_text,
                       res.status,
                       res.created_at
                FROM results res
                JOIN runs r ON r.id = res.run_id
                WHERE res.athlete_id=?
                  AND r.distance_m=?
                ORDER BY res.created_at DESC
                """,
                (athlete_id, int(distance_m)),
            ).fetchall()
        return [dict(r) for r in rows]

    def best_for_distance(self, athlete_id: int, distance_m: int) -> Optional[Dict[str, Any]]:
        with self._conn() as c:
            row = c.execute(
                """
                SELECT r.run_key,
                       r.run_started_at_text,
                       res.finish_sec,
                       res.finish_text,
                       res.status,
                       res.created_at
                FROM results res
                JOIN runs r ON r.id = res.run_id
                WHERE res.athlete_id=?
                  AND r.distance_m=?
                  AND res.finish_sec IS NOT NULL
                ORDER BY res.finish_sec ASC, res.created_at ASC
                LIMIT 1
                """,
                (athlete_id, int(distance_m)),
            ).fetchone()
        return dict(row) if row else None

    def top_athletes_in_category(self, category: str, limit: int = 30) -> List[Dict[str, Any]]:
        lim = int(limit or 30)
        if lim < 1:
            lim = 1
        if lim > 200:
            lim = 200

        with self._conn() as c:
            rows = c.execute(
                """
                WITH best_rows AS (
                    SELECT res.athlete_id,
                           res.finish_sec,
                           res.finish_text,
                           res.created_at,
                           r.run_key,
                           r.run_started_at_text,
                           ROW_NUMBER() OVER (
                               PARTITION BY res.athlete_id
                               ORDER BY res.finish_sec ASC, res.created_at ASC
                           ) AS rn
                    FROM results res
                    JOIN runs r ON r.id = res.run_id
                    JOIN athletes ax ON ax.id = res.athlete_id
                    WHERE ax.category = ?
                      AND res.finish_sec IS NOT NULL
                ),
                agg AS (
                    SELECT res.athlete_id,
                           MIN(res.finish_sec) AS best_sec,
                           COUNT(*) AS attempts,
                           COUNT(DISTINCT r.distance_m) AS distances
                    FROM results res
                    JOIN runs r ON r.id = res.run_id
                    JOIN athletes ax ON ax.id = res.athlete_id
                    WHERE ax.category = ?
                      AND res.finish_sec IS NOT NULL
                    GROUP BY res.athlete_id
                )
                SELECT a.id AS athlete_id,
                       a.bib,
                       a.name,
                       a.country,
                       agg.best_sec,
                       agg.attempts,
                       agg.distances,
                       br.finish_text AS best_text,
                       br.run_key AS best_run_key,
                       br.run_started_at_text AS best_run_started_at_text,
                       br.created_at AS best_created_at
                FROM agg
                JOIN athletes a ON a.id = agg.athlete_id
                LEFT JOIN best_rows br ON br.athlete_id = a.id AND br.rn = 1
                WHERE a.category = ?
                ORDER BY agg.best_sec ASC, agg.attempts DESC, a.name ASC, a.bib ASC
                LIMIT ?
                """,
                (category, category, category, lim),
            ).fetchall()
        return [dict(r) for r in rows]

    def athlete_full_profile(self, athlete_id: int) -> List[Dict[str, Any]]:
        aid = int(athlete_id)
        with self._conn() as c:
            rows = c.execute(
                """
                SELECT r.distance_m,
                       r.run_key,
                       r.run_started_at_text,
                       res.id AS result_id,
                       res.finish_sec,
                       res.finish_text,
                       res.status,
                       res.created_at
                FROM results res
                JOIN runs r ON r.id = res.run_id
                WHERE res.athlete_id=?
                ORDER BY r.distance_m ASC, res.created_at DESC
                """,
                (aid,),
            ).fetchall()

            splits_map: Dict[int, List[Dict[str, Any]]] = {}
            srows = c.execute(
                """
                SELECT s.result_id, s.split_num, s.split_sec, s.split_text
                FROM splits s
                JOIN results res ON res.id = s.result_id
                WHERE res.athlete_id=?
                ORDER BY s.result_id, s.split_num ASC
                """,
                (aid,),
            ).fetchall()
            for sr in srows:
                rid = int(sr["result_id"])
                splits_map.setdefault(rid, []).append({
                    "split_num": int(sr["split_num"]),
                    "split_sec": sr["split_sec"],
                    "split_text": sr["split_text"],
                })

        grouped: Dict[int, Dict[str, Any]] = {}
        for row in rows:
            d = int(row["distance_m"])
            slot = grouped.setdefault(
                d,
                {
                    "distance_m": d,
                    "attempts": 0,
                    "best": None,
                    "runs": [],
                },
            )
            rid = int(row["result_id"])
            item = {
                "run_key": row["run_key"],
                "run_started_at_text": row["run_started_at_text"],
                "finish_sec": row["finish_sec"],
                "finish_text": row["finish_text"],
                "status": row["status"],
                "created_at": row["created_at"],
                "splits": splits_map.get(rid, []),
            }
            slot["runs"].append(item)
            slot["attempts"] += 1

            fs = row["finish_sec"]
            if fs is None:
                continue
            best = slot["best"]
            if best is None or float(fs) < float(best["finish_sec"]):
                slot["best"] = item

        return [grouped[k] for k in sorted(grouped.keys())]
