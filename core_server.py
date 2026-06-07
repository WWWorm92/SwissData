from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import re
import socket
import sys
import tempfile
import threading
import time
from typing import Any, Dict, List, Optional
from urllib.parse import unquote

try:
    import serial
except Exception:
    serial = None

try:
    from openpyxl import load_workbook
except Exception:
    load_workbook = None


FREEZE_SEC = 2.0
DIST_STEP_M = 125

CC_ALIAS = {
    "МОСКВА": "МСК",
    "МСК": "МСК",
    "САНКТ-ПЕТЕРБУРГ": "СПБ",
    "САНКТПЕТЕРБУРГ": "СПБ",
    "СПБ": "СПБ",
    "БЕЛАРУСЬ": "BEL",
    "БЕЛ": "BEL",
    "BELARUS": "BEL",
    "BEL": "BEL",
    "BY": "BEL",
    "BLR": "BEL",
}

FLAG_MAP = {
    "мск": "moscow.png",
    "москва": "moscow.png",
    "спб": "spb.png",
    "санкт-петербург": "spb.png",
    "омск": "omsk.png",
    "хабаровск": "khabarovsk.png",
    "беларусь": "belarus.png",
    "иркутск": "irkutsk.png",
    "тула": "tula.png",
}

FLAG_NAME_RE = re.compile(r"^[A-Za-z0-9_\-\.]{1,64}\.(?:png|PNG)$")
MSG_RE = re.compile(r"(?:[A-Z]{0,64})?(DN|DA|DS|DI|DF)\|")
TIME_HMS_RE = re.compile(r"(\d{2}):(\d{2}):(\d{2})\.(\d{3})")
TIME_MS_RE = re.compile(r"(\d+):(\d{2})\.(\d{3})")


def cc_short(x: str) -> str:
    s = (x or "").strip().upper().replace("Ё", "Е")
    if not s:
        return ""
    key = re.sub(r"\s+", "", s)
    if key in CC_ALIAS:
        return CC_ALIAS[key]
    parts = [p for p in re.split(r"[\s\-]+", s) if p]
    if len(parts) >= 2:
        ab = "".join(p[0] for p in parts)[:3]
        if len(ab) >= 2:
            return ab
    return s[:3]


def last_split_num(a) -> int:
    mx = 0
    try:
        keys = list((a.splits or {}).keys())
    except Exception:
        keys = []
    for k in keys:
        try:
            n = int(str(k))
        except Exception:
            continue
        if n > mx:
            mx = n
    return mx


def _norm_flag_key(x) -> str:
    if x is None:
        return ""
    s = str(x).strip().lower().replace("ё", "е")
    s = re.sub(r"\s+", "", s)
    return s


def flag_for_excel_value(x) -> str:
    return FLAG_MAP.get(_norm_flag_key(x), "")


def resource_path(rel: str) -> str:
    if getattr(sys, "frozen", False):
        return os.path.join(sys._MEIPASS, rel)
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), rel)


def _atomic_write_text(path: str, text: str):
    d = os.path.dirname(path) or "."
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp_", dir=d, text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
        os.replace(tmp, path)
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


def strip_ctrl(s: str) -> str:
    if not s:
        return ""
    return "".join(ch for ch in s if ch.isprintable() or ch in "\t \n\r")


def clean_token(s: str) -> str:
    return strip_ctrl(s).replace("\r", "").replace("\n", "").strip()


def extract_first_int(token: Optional[str]) -> Optional[str]:
    if token is None:
        return None
    t = clean_token(token)
    m = re.search(r"(\d+)", t)
    if not m:
        return None
    try:
        n = int(m.group(1))
    except Exception:
        return None
    if n == 0:
        return None
    return str(n)


def parse_time_any(token: Optional[str]) -> Optional[float]:
    if token is None:
        return None
    t = clean_token(token)
    if not t:
        return None

    m = TIME_HMS_RE.search(t)
    if m:
        hh = int(m.group(1))
        mm = int(m.group(2))
        ss = int(m.group(3))
        ms = int(m.group(4))
        return hh * 3600 + mm * 60 + ss + ms / 1000.0

    m = TIME_MS_RE.search(t)
    if m:
        mm = int(m.group(1))
        ss = int(m.group(2))
        ms = int(m.group(3))
        return mm * 60 + ss + ms / 1000.0

    m = re.search(r"[+-]?\d+\.\d+", t)
    if m:
        try:
            return float(m.group(0))
        except Exception:
            return None

    m = re.search(r"[+-]?\d+", t)
    if m:
        try:
            return float(m.group(0))
        except Exception:
            return None
    return None


def find_primary_time(tokens: List[str]) -> Optional[float]:
    for tok in tokens:
        v = parse_time_any(tok)
        if v is not None:
            return v
    return None


def fmt_time(sec: Optional[float]) -> str:
    if sec is None:
        return ""
    try:
        s = float(sec)
    except Exception:
        return ""
    m = int(s // 60)
    rs = s - m * 60
    if m > 0:
        return f"{m}:{rs:06.3f}"
    return f"{s:.3f}"


def fmt_live(sec: Optional[float]) -> str:
    if sec is None:
        return ""
    try:
        s = float(sec)
    except Exception:
        return ""
    total_ms = int(max(0, round(s * 1000)))
    sec_i = (total_ms // 1000) % 60
    m = (total_ms // 60000) % 60
    h = total_ms // 3600000
    ms = total_ms % 1000
    if h > 0:
        return f"{h:d}:{m:02d}:{sec_i:02d}.{ms:03d}"
    if total_ms >= 60000:
        return f"{m:d}:{sec_i:02d}.{ms:03d}"
    return f"{total_ms/1000:.3f}"


def split_sort_key(x: str):
    try:
        return (0, int(str(x)))
    except Exception:
        return (1, str(x))


def split_stream(buffer: str):
    if not buffer:
        return [], ""

    msgs = []
    matches = list(MSG_RE.finditer(buffer))

    if matches:
        first = matches[0].start()
        if first > 0:
            buffer = buffer[first:]
            matches = list(MSG_RE.finditer(buffer))

        if len(matches) >= 2:
            for i in range(len(matches) - 1):
                a = matches[i].start()
                b = matches[i + 1].start()
                msgs.append(buffer[a:b])
            rest = buffer[matches[-1].start():]
            return msgs, rest

        start = matches[0].start()
        tail = buffer[start:]

        m_end = re.search(r"S\d{1,2}", tail)
        if m_end:
            endpos = start + m_end.end()
            msgs.append(buffer[start:endpos])
            rest = buffer[endpos:]
            return msgs, rest

        nl = buffer.find("\n", start)
        if nl != -1:
            msgs.append(buffer[start:nl + 1])
            return msgs, buffer[nl + 1:]

        return [], buffer

    if "\n" in buffer:
        parts = buffer.splitlines(True)
        for p in parts[:-1]:
            msgs.append(p)
        return msgs, parts[-1]

    return [], buffer


def parse_message(raw: str) -> Optional[Dict[str, Any]]:
    if not raw:
        return None
    raw_clean = strip_ctrl(raw).strip()
    if not raw_clean:
        return None
    m = MSG_RE.search(raw_clean)
    if not m:
        return {"type": "other", "raw": raw_clean}

    msg = m.group(1)
    body = raw_clean[m.end():]
    parts = [msg] + [clean_token(x) for x in body.split("|")]
    while parts and parts[-1] == "":
        parts.pop()

    if msg == "DN":
        race = parts[1] if len(parts) > 1 else None
        heat = parts[2] if len(parts) > 2 else None
        return {"type": "new_run", "race": race, "heat": heat, "raw": raw_clean}

    if msg == "DA":
        race = parts[1] if len(parts) > 1 else None
        heat = parts[2] if len(parts) > 2 else None
        bibs = []
        b1 = extract_first_int(parts[3]) if len(parts) > 3 else None
        b2 = extract_first_int(parts[4]) if len(parts) > 4 else None
        if b1:
            bibs.append(b1)
        if b2:
            bibs.append(b2)
        return {"type": "setup", "race": race, "heat": heat, "bibs": bibs, "raw": raw_clean}

    if msg == "DS":
        race = parts[1] if len(parts) > 1 else None
        heat = parts[2] if len(parts) > 2 else None
        bibs = []
        b1 = extract_first_int(parts[3]) if len(parts) > 3 else None
        b2 = extract_first_int(parts[4]) if len(parts) > 4 else None
        if b1:
            bibs.append(b1)
        if b2:
            bibs.append(b2)

        start_time = None
        for t in parts[1:]:
            mm = TIME_HMS_RE.search(t)
            if mm:
                start_time = mm.group(0)
                break

        return {"type": "start", "race": race, "heat": heat, "bibs": bibs, "start_time": start_time, "raw": raw_clean}

    if msg in ("DI", "DF"):
        race = parts[1] if len(parts) > 1 else None
        heat = parts[2] if len(parts) > 2 else None
        split_no = extract_first_int(parts[3]) if len(parts) > 3 else None
        bib = extract_first_int(parts[4]) if len(parts) > 4 else None
        t = find_primary_time(parts[5:12])
        etype = "split" if msg == "DI" else "finish"
        return {"type": etype, "race": race, "heat": heat, "split": split_no, "bib": bib, "time": t, "raw": raw_clean}

    return {"type": "other", "raw": raw_clean}


class Athlete:
    def __init__(self, bib: str, name: str = "", country: str = ""):
        self.bib = bib
        self.name = name or ""
        self.country = (country or "").strip().upper()
        self.splits: Dict[str, float] = {}
        self.finish: Optional[float] = None
        self.status: str = ""
        self.pause_until: float = 0.0
        self.pause_value: Optional[float] = None

    def is_paused(self) -> bool:
        return time.monotonic() < self.pause_until and self.pause_value is not None


class Run:
    def __init__(self, race: str, heat: str):
        self.race = race or "?"
        self.heat = heat or "?"
        self.key = f"{self.race}-{self.heat}"
        self.category: str = ""
        self.start_time: Optional[str] = None
        self.start_mono: Optional[float] = None
        self.athletes: Dict[str, Athlete] = {}
        self.prepared: bool = False
        self.active_bibs: List[str] = []
        self.bib_order: List[str] = []

    def ensure_athlete(self, bib: str, name: str = "", country: str = "") -> Optional[Athlete]:
        bib = clean_token(str(bib or ""))
        if not bib or bib == "0":
            return None
        if bib not in self.athletes:
            self.athletes[bib] = Athlete(bib, name=name or "", country=country or "")
            self.bib_order.append(bib)
        else:
            if name and not self.athletes[bib].name:
                self.athletes[bib].name = name
            if country and not self.athletes[bib].country:
                self.athletes[bib].country = (country or "").strip().upper()
        return self.athletes[bib]

    def finished_count(self) -> int:
        return sum(1 for a in self.athletes.values() if a.finish is not None)

    def total_count(self) -> int:
        return len(self.athletes)

    def split_ids(self) -> List[str]:
        ids = set()
        for a in self.athletes.values():
            ids.update(a.splits.keys())
        return sorted(ids, key=split_sort_key)


class MeetModel:
    def __init__(self):
        self.runs: Dict[str, Run] = {}
        self.current_key: Optional[str] = None
        self.bib_names: Dict[str, str] = {}
        self.bib_country: Dict[str, str] = {}
        self.race_shift: int = 0

    def set_bib_meta(self, names: Dict[str, str], countries: Dict[str, str]):
        self.bib_names = dict(names or {})
        self.bib_country = {str(k): (v or "").strip().upper() for k, v in (countries or {}).items()}

    def ensure_run(self, race, heat) -> Run:
        key = f"{race or '?'}-{heat or '?'}"
        if key not in self.runs:
            self.runs[key] = Run(race, heat)
        return self.runs[key]

    def _attach_category(self, run: Run, category: Any):
        cat = str(category or "").strip()
        if not cat:
            return
        if not run.category:
            run.category = cat
            return
        if run.total_count() == 0 and not run.start_time:
            run.category = cat

    def _shifted_race(self, race: Any) -> Any:
        if race is None:
            return race
        rs = str(race).strip()
        if not rs.isdigit():
            return race
        return str(int(rs) + int(self.race_shift))

    def _maybe_roll_session(self, evt_type: str, race: Any, heat: Any):
        if evt_type not in ("new_run", "setup", "start"):
            return
        rs = str(race).strip() if race is not None else ""
        hs = str(heat).strip() if heat is not None else ""
        if rs != "1" or hs != "1":
            return
        cur_key = f"{int(rs) + int(self.race_shift)}-{hs}"
        if cur_key not in self.runs:
            return
        if self.current_key == cur_key and evt_type != "new_run":
            return
        while True:
            k = f"{int(rs) + int(self.race_shift)}-{hs}"
            if k in self.runs:
                self.race_shift += 1
                continue
            break

    def _pick_run_for_bibs(self, race, heat, bibs: List[str]) -> Optional[Run]:
        cur = self.runs.get(self.current_key) if self.current_key else None
        if cur and bibs:
            if set(cur.active_bibs) == set(bibs) or all(b in cur.athletes for b in bibs):
                return cur
        if bibs:
            for r in self.runs.values():
                if (race is None or r.race == (race or r.race)) and (set(r.active_bibs) == set(bibs) or all(b in r.athletes for b in bibs)):
                    return r
        return None

    def apply(self, evt: Dict[str, Any]) -> Optional[str]:
        t = evt.get("type")
        race = evt.get("race")
        heat = evt.get("heat")
        evt_cat = evt.get("category")

        self._maybe_roll_session(str(t), race, heat)
        race = self._shifted_race(race)

        if t == "new_run":
            run = self.ensure_run(race, heat)
            self._attach_category(run, evt_cat)
            self.current_key = run.key
            return run.key

        if t == "setup":
            run = self.ensure_run(race, heat)
            self._attach_category(run, evt_cat)
            run.prepared = True
            bibs = evt.get("bibs") or []
            bibs = [b for b in bibs if b and str(b).strip() not in ("0", "")]
            if bibs:
                run.active_bibs = bibs[:2]
                for b in bibs:
                    a = run.ensure_athlete(b, self.bib_names.get(str(b), ""), self.bib_country.get(str(b), ""))
                    if a:
                        a.status = "готов"
                tail = [b for b in run.bib_order if b not in bibs]
                run.bib_order = list(bibs) + tail
            self.current_key = run.key
            return run.key

        if t == "start":
            bibs = evt.get("bibs") or []
            bibs = [b for b in bibs if b and str(b).strip() not in ("0", "")]
            run = self._pick_run_for_bibs(race, heat, bibs) or self.ensure_run(race, heat)
            self._attach_category(run, evt_cat)
            run.start_time = evt.get("start_time") or run.start_time
            run.start_mono = time.monotonic()
            if bibs:
                run.active_bibs = bibs[:2]
                for b in bibs:
                    a = run.ensure_athlete(b, self.bib_names.get(str(b), ""), self.bib_country.get(str(b), ""))
                    if a:
                        a.status = "в заезде"
                        a.pause_until = 0.0
                        a.pause_value = None
                tail = [b for b in run.bib_order if b not in bibs]
                run.bib_order = list(bibs) + tail
            self.current_key = run.key
            return run.key

        if t in ("split", "finish"):
            run = self.ensure_run(race, heat)
            self._attach_category(run, evt_cat)
            ev_time = evt.get("time")
            if run.start_mono is None and ev_time is not None:
                run.start_mono = time.monotonic() - float(ev_time)
            bib = evt.get("bib")
            a = run.ensure_athlete(bib, self.bib_names.get(str(bib), ""), self.bib_country.get(str(bib), ""))
            if not a:
                self.current_key = run.key
                return run.key
            now = time.monotonic()
            if t == "split":
                split_no = clean_token(str(evt.get("split") or ""))
                if split_no and ev_time is not None:
                    a.splits[split_no] = ev_time
                    a.status = f"отсечка {split_no}"
                else:
                    a.status = "отсечка"
                a.pause_value = ev_time
                a.pause_until = now + FREEZE_SEC
            else:
                a.finish = ev_time
                a.status = "финиш"
                a.pause_value = ev_time
                a.pause_until = float("inf")
            self.current_key = run.key
            return run.key

        return None


class TcpJsonlServer:
    def __init__(self, host: str, port: int, on_error=None):
        self.host = host
        self.port = port
        self.on_error = on_error
        self._stop = threading.Event()
        self._srv: Optional[socket.socket] = None
        self._clients: List[socket.socket] = []
        self._lock = threading.Lock()
        self._thr: Optional[threading.Thread] = None
        self._last_bytes: Optional[bytes] = None

    def start(self):
        if self._thr and self._thr.is_alive():
            return
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.host, self.port))
            s.listen(16)
            s.settimeout(0.5)
            self._srv = s
        except Exception as e:
            if self.on_error:
                self.on_error(f"bind/listen failed: {e}")
            return
        self._stop.clear()
        self._thr = threading.Thread(target=self._accept_loop, daemon=True)
        self._thr.start()

    def stop(self):
        self._stop.set()
        try:
            if self._srv:
                self._srv.close()
        except Exception:
            pass
        with self._lock:
            for c in self._clients:
                try:
                    c.close()
                except Exception:
                    pass
            self._clients.clear()

    def _encode(self, obj: Dict[str, Any]) -> bytes:
        return (json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8", errors="ignore")

    def set_last(self, obj: Dict[str, Any]):
        try:
            self._last_bytes = self._encode(obj)
        except Exception:
            self._last_bytes = None

    def _accept_loop(self):
        s = self._srv
        if not s:
            return
        while not self._stop.is_set():
            try:
                c, _ = s.accept()
                c.setblocking(True)
                with self._lock:
                    self._clients.append(c)
                    lastb = self._last_bytes
                if lastb:
                    try:
                        c.sendall(lastb)
                    except Exception:
                        with self._lock:
                            try:
                                self._clients.remove(c)
                            except Exception:
                                pass
                        try:
                            c.close()
                        except Exception:
                            pass
            except socket.timeout:
                continue
            except Exception as e:
                if self.on_error:
                    self.on_error(f"accept loop error: {e}")
                break

    def broadcast(self, obj: Dict[str, Any]):
        self.set_last(obj)
        data = self._last_bytes
        if not data:
            return
        dead = []
        with self._lock:
            for c in self._clients:
                try:
                    c.sendall(data)
                except Exception:
                    dead.append(c)
            for c in dead:
                try:
                    c.close()
                except Exception:
                    pass
                try:
                    self._clients.remove(c)
                except Exception:
                    pass


class TcpStateServer:
    def __init__(self, host: str, port: int, on_error=None):
        self.host = host
        self.port = port
        self.on_error = on_error
        self._stop = threading.Event()
        self._srv: Optional[socket.socket] = None
        self._clients: List[socket.socket] = []
        self._lock = threading.Lock()
        self._thr: Optional[threading.Thread] = None
        self._last_state_bytes: Optional[bytes] = None

    def start(self):
        if self._thr and self._thr.is_alive():
            return
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.host, self.port))
            s.listen(16)
            s.settimeout(0.5)
            self._srv = s
        except Exception as e:
            if self.on_error:
                self.on_error(f"bind/listen failed: {e}")
            return
        self._stop.clear()
        self._thr = threading.Thread(target=self._accept_loop, daemon=True)
        self._thr.start()

    def stop(self):
        self._stop.set()
        try:
            if self._srv:
                self._srv.close()
        except Exception:
            pass
        with self._lock:
            for c in self._clients:
                try:
                    c.close()
                except Exception:
                    pass
            self._clients.clear()

    def _encode_state(self, state: Dict[str, Any]) -> bytes:
        line = json.dumps({"type": "state", "state": state}, ensure_ascii=False) + "\n"
        return line.encode("utf-8", errors="ignore")

    def set_last_state(self, state: Dict[str, Any]):
        try:
            self._last_state_bytes = self._encode_state(state)
        except Exception:
            self._last_state_bytes = None

    def _accept_loop(self):
        s = self._srv
        if not s:
            return
        while not self._stop.is_set():
            try:
                c, _ = s.accept()
                c.setblocking(True)
                try:
                    hello = (json.dumps({"type": "hello", "v": 1}, ensure_ascii=False) + "\n").encode("utf-8")
                    c.sendall(hello)
                except Exception:
                    try:
                        c.close()
                    except Exception:
                        pass
                    continue
                with self._lock:
                    self._clients.append(c)
                    lastb = self._last_state_bytes
                if lastb:
                    try:
                        c.sendall(lastb)
                    except Exception:
                        with self._lock:
                            try:
                                self._clients.remove(c)
                            except Exception:
                                pass
                        try:
                            c.close()
                        except Exception:
                            pass
            except socket.timeout:
                continue
            except Exception as e:
                if self.on_error:
                    self.on_error(f"accept loop error: {e}")
                break

    def broadcast_state(self, state: Dict[str, Any]):
        self.set_last_state(state)
        data = self._last_state_bytes
        if not data:
            return
        dead = []
        with self._lock:
            for c in self._clients:
                try:
                    c.sendall(data)
                except Exception:
                    dead.append(c)
            for c in dead:
                try:
                    c.close()
                except Exception:
                    pass
                try:
                    self._clients.remove(c)
                except Exception:
                    pass


class ReaderThread(threading.Thread):
    def __init__(self, q, stop_evt, port=None, baud=9600, replay_path=None):
        super().__init__(daemon=True)
        self.q = q
        self.stop_evt = stop_evt
        self.port = port
        self.baud = baud
        self.replay_path = replay_path
        self.ser = None

    def _emit_evt(self, evt):
        self.q.put({"kind": "evt", "data": evt})

    def run(self):
        if self.replay_path:
            self._run_replay()
        else:
            self._run_serial()

    def _drain_buf(self, buf: str):
        while True:
            msgs, buf2 = split_stream(buf)
            if not msgs:
                return buf
            for raw in msgs:
                evt = parse_message(raw)
                if evt:
                    self._emit_evt(evt)
            buf = buf2

    def _run_replay(self):
        try:
            with open(self.replay_path, "r", encoding="utf-8", errors="ignore") as f:
                buf = ""
                while not self.stop_evt.is_set():
                    chunk = f.read(512)
                    if not chunk:
                        break
                    buf += chunk
                    buf = self._drain_buf(buf)
                    time.sleep(0.002)
                tail = buf.strip()
                if tail:
                    evt = parse_message(tail)
                    if evt:
                        self._emit_evt(evt)
        except Exception as e:
            self.q.put({"kind": "err", "data": str(e)})

    def _run_serial(self):
        if serial is None:
            self.q.put({"kind": "err", "data": "pyserial не установлен"})
            return
        try:
            self.ser = serial.Serial(
                self.port,
                self.baud,
                timeout=0.2,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                xonxoff=False,
                rtscts=False,
                dsrdtr=False,
            )
        except Exception as e:
            self.q.put({"kind": "err", "data": f"Не открыл порт: {e}"})
            return

        buf = ""
        last_byte_ts = time.time()
        try:
            while not self.stop_evt.is_set():
                try:
                    n = self.ser.in_waiting
                except Exception:
                    n = 0
                data = self.ser.read(n if n else 1)
                if data:
                    last_byte_ts = time.time()
                    s = data.decode("ascii", errors="ignore")
                    buf += s
                    buf = self._drain_buf(buf)
                else:
                    if buf and (time.time() - last_byte_ts) > 0.35:
                        tail = buf.strip()
                        if tail and MSG_RE.search(tail):
                            evt = parse_message(tail)
                            if evt:
                                self._emit_evt(evt)
                        buf = ""
        except Exception as e:
            self.q.put({"kind": "err", "data": str(e)})
        finally:
            try:
                self.ser.close()
            except Exception:
                pass


class OverlayHttp:
    def __init__(self, app, host="0.0.0.0", port=8099, overlay_html_path=None, flags_dir=None):
        self.app = app
        self.host = host
        self.port = port
        self.overlay_html_path = overlay_html_path
        self.flags_dir = flags_dir
        self.httpd = None
        self.thr = None

    def start(self):
        app = self.app
        overlay_html_path = self.overlay_html_path
        flags_dir = self.flags_dir

        class H(BaseHTTPRequestHandler):
            def _send(self, code, ctype, data: bytes):
                self.send_response(code)
                self.send_header("Content-Type", ctype)
                self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                path = self.path.split("?", 1)[0]
                if path in ("/", "/overlay_test.html"):
                    if not overlay_html_path or not os.path.isfile(overlay_html_path):
                        return self._send(404, "text/plain; charset=utf-8", b"overlay.html not found")
                    try:
                        with open(overlay_html_path, "rb") as f:
                            data = f.read()
                        return self._send(200, "text/html; charset=utf-8", data)
                    except Exception:
                        return self._send(500, "text/plain; charset=utf-8", b"cannot read overlay.html")

                if path.startswith("/flags/"):
                    if not flags_dir:
                        return self._send(404, "text/plain; charset=utf-8", b"flags dir not set")
                    fn = os.path.basename(unquote(path[len("/flags/"):]))
                    if not FLAG_NAME_RE.match(fn):
                        return self._send(404, "text/plain; charset=utf-8", b"bad flag name")
                    fp = os.path.join(flags_dir, fn)
                    if not os.path.isfile(fp):
                        return self._send(404, "text/plain; charset=utf-8", b"flag not found")
                    try:
                        with open(fp, "rb") as f:
                            data = f.read()
                        return self._send(200, "image/png", data)
                    except Exception:
                        return self._send(500, "text/plain; charset=utf-8", b"cannot read flag")

                if self.path.startswith("/state.json"):
                    try:
                        with app._obs_lock:
                            payload = dict(app._obs_payload)
                        data = (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")
                        return self._send(200, "application/json; charset=utf-8", data)
                    except Exception:
                        return self._send(500, "application/json; charset=utf-8", b"{}")

                return self._send(404, "text/plain; charset=utf-8", b"not found")

            def log_message(self, _format, *_args):
                return

        try:
            self.httpd = ThreadingHTTPServer((self.host, self.port), H)
        except Exception as e:
            try:
                app.q.put({"kind": "err", "data": f"OVERLAY HTTP: не стартанул на {self.host}:{self.port} ({e})"})
            except Exception:
                pass
            self.httpd = None
            return

        self.thr = threading.Thread(target=self.httpd.serve_forever, kwargs={"poll_interval": 0.2}, daemon=True)
        self.thr.start()
        try:
            app.q.put({"kind": "evt", "data": {"type": "other", "raw": f"OVERLAY HTTP: запущен http://{self.host}:{self.port}/overlay_test.html"}})
        except Exception:
            pass

    def stop(self):
        try:
            if self.httpd:
                self.httpd.shutdown()
                self.httpd.server_close()
        except Exception:
            pass
