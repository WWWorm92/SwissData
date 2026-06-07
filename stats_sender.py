import json
import queue
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional


class AsyncStatsSender:
    def __init__(self, endpoint: str, enabled: bool = True):
        self.endpoint = self._normalize_endpoint(endpoint)
        self.enabled = bool(enabled) and bool(self.endpoint)
        self._q: queue.Queue = queue.Queue(maxsize=5000)
        self._stop = threading.Event()
        self._thr: Optional[threading.Thread] = None
        self._last_error_ts = 0.0

    def start(self):
        if not self.enabled:
            return
        if self._thr and self._thr.is_alive():
            return
        self._stop.clear()
        self._thr = threading.Thread(target=self._loop, daemon=True)
        self._thr.start()

    def stop(self):
        self._stop.set()

    def set_endpoint(self, endpoint: str):
        self.endpoint = self._normalize_endpoint(endpoint)
        self.enabled = bool(self.endpoint)

    def _normalize_endpoint(self, endpoint: str) -> str:
        ep = str(endpoint or "").strip()
        if not ep:
            return ""
        if "://" not in ep:
            ep = "http://" + ep
        try:
            p = urllib.parse.urlparse(ep)
            if p.scheme not in ("http", "https"):
                return ""
            if not p.netloc:
                return ""
            path = (p.path or "").strip()
            if not path or path == "/":
                p = p._replace(path="/ingest/result")
            ep = urllib.parse.urlunparse(p)
        except Exception:
            return ""
        return ep

    def _with_ingest_path(self, endpoint: str) -> str:
        try:
            p = urllib.parse.urlparse(str(endpoint or "").strip())
            path = (p.path or "").rstrip("/")
            if path.endswith("/ingest/result"):
                return urllib.parse.urlunparse(p)
            p = p._replace(path=(path + "/ingest/result") if path else "/ingest/result")
            return urllib.parse.urlunparse(p)
        except Exception:
            return endpoint

    def send(self, payload: Dict[str, Any]):
        if not self.enabled:
            return
        try:
            self._q.put_nowait(dict(payload or {}))
        except Exception:
            pass

    def _loop(self):
        while not self._stop.is_set():
            try:
                payload = self._q.get(timeout=0.2)
            except queue.Empty:
                continue

            if not self.endpoint:
                continue
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            req = urllib.request.Request(
                self.endpoint,
                data=data,
                headers={"Content-Type": "application/json; charset=utf-8"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=1.5) as _resp:
                    pass
            except urllib.error.HTTPError as e:
                body = ""
                try:
                    body = e.read().decode("utf-8", errors="ignore")
                except Exception:
                    body = ""
                txt = body.lower()
                if "invalid url" in txt and not self.endpoint.rstrip("/").endswith("/ingest/result"):
                    fixed = self._with_ingest_path(self.endpoint)
                    self.endpoint = self._normalize_endpoint(fixed)
            except Exception:
                now = time.time()
                if (now - self._last_error_ts) >= 10.0:
                    self._last_error_ts = now
