# clien.py
# SwissTiming Quantum Client (idealized)
# - Connects to Quantum server over TCP (JSON Lines)
# - Shows runs + athletes, supports per-run categories to avoid bib collisions across categories
# - Local roster per category (bib -> name/country) loaded from Excel/CSV and used to override/display names
# - Export selected run / all runs to CSV / JSON snapshot
#
# Protocol:
#   server sends JSONL lines:
#     {"type":"hello","v":1}
#     {"type":"state","state":{...}}  where state is MeetModel._model_to_state()

import argparse
import csv
import json
import os
import queue
import socket
import threading
import time
import tkinter as tk
from dataclasses import dataclass, field
from pathlib import Path
from tkinter import filedialog, messagebox
from tkinter import ttk
from typing import Any, Dict, List, Optional, Tuple

try:
    from openpyxl import load_workbook
except Exception:
    load_workbook = None


APP_NAME = "Quantum Client"
DEFAULT_CATEGORIES = ["Мужчины", "Женщины", "Юниоры", "Девушки", "Мастерс"]
DIST_PER_SPLIT_M = 125


def now_ts() -> float:
    return time.time()


def safe_int_str(x: Any) -> str:
    s = str(x).strip()
    if not s:
        return ""
    if s.isdigit():
        return s
    num = ""
    for ch in s:
        if ch.isdigit():
            num += ch
        elif num:
            break
    return num


def split_sort_key(x: str):
    x = str(x).strip()
    if x.isdigit():
        return (0, int(x))
    return (1, x)


def fmt_time(sec: Any) -> str:
    if sec is None:
        return ""
    try:
        f = float(sec)
    except Exception:
        return str(sec)
    sign = "-" if f < 0 else ""
    f = abs(f)
    total_ms = int(round(f * 1000))
    s = (total_ms // 1000) % 60
    m = (total_ms // 60000) % 60
    h = total_ms // 3600000
    ms = total_ms % 1000
    if h > 0:
        return f"{sign}{h:d}:{m:02d}:{s:02d}.{ms:03d}"
    if total_ms >= 60000:
        return f"{sign}{m:d}:{s:02d}.{ms:03d}"
    return f"{sign}{total_ms/1000:.3f}"


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def app_data_dir() -> Path:
    base = os.environ.get("APPDATA") or str(Path.home())
    return Path(base) / ".quantum_client"


@dataclass
class Settings:
    host: str = "127.0.0.1"
    port: int = 9876
    auto_reconnect: bool = True
    override_server_names: bool = True
    show_distance: bool = True
    categories: List[str] = field(default_factory=lambda: list(DEFAULT_CATEGORIES))
    run_categories: Dict[str, str] = field(default_factory=dict)  # run_key -> category


class SettingsStore:
    def __init__(self):
        self.dir = app_data_dir()
        ensure_dir(self.dir)
        self.path = self.dir / "settings.json"

    def load(self) -> Settings:
        if not self.path.exists():
            return Settings()
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            s = Settings()
            s.host = str(data.get("host", s.host))
            s.port = int(data.get("port", s.port))
            s.auto_reconnect = bool(data.get("auto_reconnect", s.auto_reconnect))
            s.override_server_names = bool(data.get("override_server_names", s.override_server_names))
            s.show_distance = bool(data.get("show_distance", s.show_distance))
            cats = data.get("categories")
            if isinstance(cats, list) and cats:
                s.categories = [str(x) for x in cats if str(x).strip()]
            rc = data.get("run_categories")
            if isinstance(rc, dict):
                s.run_categories = {str(k): str(v) for k, v in rc.items() if str(k).strip()}
            return s
        except Exception:
            return Settings()

    def save(self, s: Settings):
        try:
            data = {
                "host": s.host,
                "port": s.port,
                "auto_reconnect": s.auto_reconnect,
                "override_server_names": s.override_server_names,
                "show_distance": s.show_distance,
                "categories": s.categories,
                "run_categories": s.run_categories,
            }
            self.path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass


class RosterStore:
    """
    roster.json structure:
      {
        "categories": {
          "Мужчины": {"97": {"name":"...", "country":"RUS"}, ...},
          ...
        }
      }
    """
    def __init__(self):
        self.dir = app_data_dir()
        ensure_dir(self.dir)
        self.path = self.dir / "roster.json"
        self.data: Dict[str, Dict[str, Dict[str, str]]] = {}
        self.load()

    def load(self):
        self.data = {}
        if not self.path.exists():
            return
        try:
            j = json.loads(self.path.read_text(encoding="utf-8"))
            cats = j.get("categories", {})
            if isinstance(cats, dict):
                for cat, mp in cats.items():
                    if not isinstance(mp, dict):
                        continue
                    cat_s = str(cat).strip()
                    if not cat_s:
                        continue
                    self.data[cat_s] = {}
                    for bib, meta in mp.items():
                        bib_s = safe_int_str(bib)
                        if not bib_s:
                            continue
                        if isinstance(meta, dict):
                            name = str(meta.get("name", "")).strip()
                            country = str(meta.get("country", "")).strip().upper()
                        else:
                            name = str(meta).strip()
                            country = ""
                        self.data[cat_s][bib_s] = {"name": name, "country": country}
        except Exception:
            self.data = {}

    def save(self):
        try:
            out = {"categories": self.data}
            self.path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    def ensure_category(self, cat: str):
        cat = str(cat).strip()
        if not cat:
            return
        if cat not in self.data:
            self.data[cat] = {}

    def set_entry(self, cat: str, bib: str, name: str, country: str = ""):
        cat = str(cat).strip()
        bib = safe_int_str(bib)
        if not cat or not bib:
            return
        self.ensure_category(cat)
        self.data[cat][bib] = {"name": (name or "").strip(), "country": (country or "").strip().upper()}

    def get_entry(self, cat: str, bib: str) -> Optional[Dict[str, str]]:
        cat = str(cat).strip()
        bib = safe_int_str(bib)
        if not cat or not bib:
            return None
        return self.data.get(cat, {}).get(bib)

    def delete_entry(self, cat: str, bib: str):
        cat = str(cat).strip()
        bib = safe_int_str(bib)
        if not cat or not bib:
            return
        try:
            del self.data[cat][bib]
        except Exception:
            pass

    def clear_category(self, cat: str):
        cat = str(cat).strip()
        if not cat:
            return
        self.data[cat] = {}


class TcpClientThread(threading.Thread):
    def __init__(self, host: str, port: int, out_queue: "queue.Queue[Dict[str, Any]]", stop_evt: threading.Event):
        super().__init__(daemon=True)
        self.host = host
        self.port = port
        self.q = out_queue
        self.stop_evt = stop_evt
        self.sock: Optional[socket.socket] = None

    def run(self):
        self._emit({"kind": "status", "ok": False, "text": "Подключение..."})
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5.0)
            s.connect((self.host, self.port))
            s.settimeout(0.8)
            self.sock = s
        except Exception as e:
            self._emit({"kind": "error", "text": f"Не удалось подключиться: {e}"})
            self._emit({"kind": "status", "ok": False, "text": "Отключено"})
            return

        self._emit({"kind": "status", "ok": True, "text": f"TCP {self.host}:{self.port}"})

        buf = b""
        try:
            while not self.stop_evt.is_set():
                try:
                    chunk = self.sock.recv(4096)
                    if not chunk:
                        raise ConnectionError("соединение закрыто")
                    buf += chunk
                except socket.timeout:
                    continue

                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        msg = json.loads(line.decode("utf-8", errors="ignore"))
                        self._emit({"kind": "msg", "data": msg, "raw": line.decode("utf-8", errors="ignore")})
                    except Exception:
                        self._emit({"kind": "raw", "text": line.decode("utf-8", errors="ignore")})
        except Exception as e:
            if not self.stop_evt.is_set():
                self._emit({"kind": "error", "text": f"TCP ошибка: {e}"})
        finally:
            try:
                if self.sock:
                    self.sock.close()
            except Exception:
                pass
            self.sock = None
            self._emit({"kind": "status", "ok": False, "text": "Отключено"})

    def _emit(self, item: Dict[str, Any]):
        try:
            self.q.put(item, block=False)
        except Exception:
            pass


class CategoryDialog(tk.Toplevel):
    def __init__(self, master, categories: List[str]):
        super().__init__(master)
        self.title("Категории")
        self.resizable(False, False)
        self.categories = list(categories)
        self.result: Optional[List[str]] = None

        self.lb = tk.Listbox(self, height=10, width=38)
        self.lb.pack(padx=12, pady=(12, 8), fill="both", expand=False)
        for c in self.categories:
            self.lb.insert("end", c)

        row = ttk.Frame(self)
        row.pack(padx=12, pady=(0, 10), fill="x")

        ttk.Button(row, text="Добавить", command=self._add).pack(side="left")
        ttk.Button(row, text="Переименовать", command=self._rename).pack(side="left", padx=(8, 0))
        ttk.Button(row, text="Удалить", command=self._delete).pack(side="left", padx=(8, 0))

        row2 = ttk.Frame(self)
        row2.pack(padx=12, pady=(0, 12), fill="x")
        ttk.Button(row2, text="Отмена", command=self._cancel).pack(side="right")
        ttk.Button(row2, text="OK", command=self._ok).pack(side="right", padx=(0, 8))

        self.grab_set()
        self.protocol("WM_DELETE_WINDOW", self._cancel)

    def _prompt(self, title: str, initial: str = "") -> Optional[str]:
        d = tk.Toplevel(self)
        d.title(title)
        d.resizable(False, False)
        ttk.Label(d, text=title).pack(padx=12, pady=(12, 6), anchor="w")
        v = tk.StringVar(value=initial)
        e = ttk.Entry(d, textvariable=v, width=38)
        e.pack(padx=12, pady=(0, 10))
        e.focus_set()

        out = {"val": None}

        def ok():
            s = v.get().strip()
            out["val"] = s if s else None
            d.destroy()

        def cancel():
            out["val"] = None
            d.destroy()

        r = ttk.Frame(d)
        r.pack(padx=12, pady=(0, 12), fill="x")
        ttk.Button(r, text="Отмена", command=cancel).pack(side="right")
        ttk.Button(r, text="OK", command=ok).pack(side="right", padx=(0, 8))
        d.grab_set()
        d.wait_window()
        return out["val"]

    def _sel_index(self) -> Optional[int]:
        sel = self.lb.curselection()
        if not sel:
            return None
        return int(sel[0])

    def _add(self):
        s = self._prompt("Новая категория")
        if not s:
            return
        if s in self.categories:
            messagebox.showerror("Ошибка", "Такая категория уже есть")
            return
        self.categories.append(s)
        self.lb.insert("end", s)

    def _rename(self):
        idx = self._sel_index()
        if idx is None:
            return
        cur = self.categories[idx]
        s = self._prompt("Переименовать", initial=cur)
        if not s or s == cur:
            return
        if s in self.categories:
            messagebox.showerror("Ошибка", "Такая категория уже есть")
            return
        self.categories[idx] = s
        self.lb.delete(idx)
        self.lb.insert(idx, s)

    def _delete(self):
        idx = self._sel_index()
        if idx is None:
            return
        cur = self.categories[idx]
        if messagebox.askyesno("Удалить", f"Удалить категорию: {cur}?"):
            self.categories.pop(idx)
            self.lb.delete(idx)

    def _ok(self):
        out = [c for c in self.categories if c.strip()]
        if not out:
            messagebox.showerror("Ошибка", "Нужна хотя бы одна категория")
            return
        self.result = out
        self.destroy()

    def _cancel(self):
        self.result = None
        self.destroy()


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_NAME)
        self.geometry("1440x900")
        self.minsize(1150, 720)

        self.settings_store = SettingsStore()
        self.settings = self.settings_store.load()
        self.roster = RosterStore()

        self._q: "queue.Queue[Dict[str, Any]]" = queue.Queue()
        self._stop_evt = threading.Event()
        self._thr: Optional[TcpClientThread] = None
        self._connected = False
        self._last_state_ts = 0.0

        self.state: Dict[str, Any] = {}
        self.selected_run_key: Optional[str] = None

        self.host_var = tk.StringVar(value=self.settings.host)
        self.port_var = tk.StringVar(value=str(self.settings.port))
        self.status_var = tk.StringVar(value="Отключено")
        self.auto_reconnect_var = tk.BooleanVar(value=self.settings.auto_reconnect)
        self.override_names_var = tk.BooleanVar(value=self.settings.override_server_names)
        self.show_distance_var = tk.BooleanVar(value=self.settings.show_distance)

        self.run_filter_var = tk.StringVar(value="")
        self.ath_filter_var = tk.StringVar(value="")
        self.category_var = tk.StringVar(value=(self.settings.categories[0] if self.settings.categories else ""))
        self.run_category_var = tk.StringVar(value="")

        self._build_style()
        self._build_ui()

        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.after(80, self._pump)
        self.after(600, self._auto_save_tick)

    def _build_style(self):
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except Exception:
            pass

        bg = "#0f1117"
        panel = "#151a23"
        panel2 = "#111621"
        fg = "#e6e6e6"
        muted = "#a8b0bf"
        accent = "#3aa0ff"
        ok = "#7ee787"
        danger = "#ff5c5c"
        line = "#242b3a"

        self.colors = {
            "bg": bg,
            "panel": panel,
            "panel2": panel2,
            "fg": fg,
            "muted": muted,
            "accent": accent,
            "ok": ok,
            "danger": danger,
            "line": line,
            "odd": "#121826",
            "even": "#0f1522",
            "head": "#1b2230",
            "select": "#243044",
        }

        self.configure(bg=bg)
        style.configure(".", background=bg, foreground=fg)
        style.configure("TFrame", background=bg)
        style.configure("Card.TFrame", background=panel)
        style.configure("Card2.TFrame", background=panel2)
        style.configure("TLabel", background=bg, foreground=fg)
        style.configure("Muted.TLabel", background=bg, foreground=muted)
        style.configure("H1.TLabel", background=bg, foreground=fg, font=("Segoe UI", 16, "bold"))
        style.configure("H2.TLabel", background=bg, foreground=fg, font=("Segoe UI", 13, "bold"))

        style.configure("TButton", background=panel, foreground=fg, borderwidth=0, padding=(12, 9))
        style.map("TButton", background=[("active", self.colors["head"]), ("pressed", self.colors["select"])])

        style.configure("Accent.TButton", background=accent, foreground="#0b0d12")
        style.map("Accent.TButton", background=[("active", "#5bb3ff"), ("pressed", "#2f8fe6")])

        style.configure("TEntry", fieldbackground=panel2, foreground=fg)
        style.configure("TCombobox", fieldbackground=panel2, background=panel2, foreground=fg, arrowcolor=fg)

        style.configure("Treeview", background=panel2, fieldbackground=panel2, foreground=fg, rowheight=34, borderwidth=0)
        style.map("Treeview", background=[("selected", self.colors["select"])], foreground=[("selected", fg)])
        style.configure("Treeview.Heading", background=self.colors["head"], foreground=fg, relief="flat",
                        font=("Segoe UI", 11, "bold"), padding=(10, 10))

    def _build_ui(self):
        c = self.colors
        root = ttk.Frame(self, style="TFrame")
        root.pack(fill="both", expand=True, padx=14, pady=14)

        top = ttk.Frame(root, style="TFrame")
        top.pack(fill="x", pady=(0, 10))
        ttk.Label(top, text="SwissTiming Quantum Client", style="H1.TLabel").pack(side="left")

        bar = ttk.Frame(root, style="Card.TFrame")
        bar.pack(fill="x", pady=(0, 12))
        inner = ttk.Frame(bar, style="Card.TFrame")
        inner.pack(fill="x", padx=12, pady=12)

        ttk.Label(inner, text="Host", style="Muted.TLabel").pack(side="left")
        ttk.Entry(inner, textvariable=self.host_var, width=16).pack(side="left", padx=(6, 12))
        ttk.Label(inner, text="Port", style="Muted.TLabel").pack(side="left")
        ttk.Entry(inner, textvariable=self.port_var, width=7).pack(side="left", padx=(6, 12))

        ttk.Button(inner, text="Подключить", style="Accent.TButton", command=self.connect).pack(side="left", padx=(0, 8))
        ttk.Button(inner, text="Отключить", command=self.disconnect).pack(side="left")

        ttk.Checkbutton(inner, text="Авто-переподключение", variable=self.auto_reconnect_var).pack(side="left", padx=(16, 0))
        ttk.Checkbutton(inner, text="Подменять имена из состава", variable=self.override_names_var).pack(side="left", padx=(16, 0))
        ttk.Checkbutton(inner, text="Показывать дистанцию", variable=self.show_distance_var, command=self._refresh_views).pack(side="left", padx=(16, 0))

        sw = ttk.Frame(inner, style="Card.TFrame")
        sw.pack(side="right")
        self.status_dot = tk.Canvas(sw, width=14, height=14, bg=c["panel"], highlightthickness=0)
        self.status_dot.pack(side="left", padx=(0, 8))
        self._dot_id = self.status_dot.create_oval(2, 2, 12, 12, fill=c["danger"], outline=c["danger"])
        ttk.Label(sw, textvariable=self.status_var, style="H2.TLabel").pack(side="left")

        self.nb = ttk.Notebook(root)
        self.nb.pack(fill="both", expand=True)

        self.tab_runs = ttk.Frame(self.nb, style="TFrame")
        self.tab_roster = ttk.Frame(self.nb, style="TFrame")
        self.tab_export = ttk.Frame(self.nb, style="TFrame")
        self.tab_log = ttk.Frame(self.nb, style="TFrame")

        self.nb.add(self.tab_runs, text="Заезды")
        self.nb.add(self.tab_roster, text="Состав")
        self.nb.add(self.tab_export, text="Экспорт")
        self.nb.add(self.tab_log, text="Лог")

        self._build_runs_tab()
        self._build_roster_tab()
        self._build_export_tab()
        self._build_log_tab()

    def _build_runs_tab(self):
        c = self.colors

        pan = ttk.Panedwindow(self.tab_runs, orient="horizontal")
        pan.pack(fill="both", expand=True)

        left = ttk.Frame(pan, style="Card.TFrame")
        right = ttk.Frame(pan, style="Card.TFrame")
        pan.add(left, weight=1)
        pan.add(right, weight=3)

        lpad = ttk.Frame(left, style="Card.TFrame")
        lpad.pack(fill="both", expand=True, padx=12, pady=12)
        head = ttk.Frame(lpad, style="Card.TFrame")
        head.pack(fill="x")
        ttk.Label(head, text="Заезды", style="H2.TLabel").pack(side="left")
        ttk.Button(head, text="Категории…", command=self._edit_categories).pack(side="right")

        flt = ttk.Frame(lpad, style="Card.TFrame")
        flt.pack(fill="x", pady=(10, 8))
        ttk.Label(flt, text="Фильтр", style="Muted.TLabel").pack(side="left")
        ttk.Entry(flt, textvariable=self.run_filter_var).pack(side="left", fill="x", expand=True, padx=(8, 0))
        self.run_filter_var.trace_add("write", lambda *_: self._render_runs())

        cols = ("run", "cat", "start", "ath", "fin")
        self.runs_tv = ttk.Treeview(lpad, columns=cols, show="headings", height=18)
        self.runs_tv.heading("run", text="Заезд")
        self.runs_tv.heading("cat", text="Кат.")
        self.runs_tv.heading("start", text="Старт")
        self.runs_tv.heading("ath", text="Участн.")
        self.runs_tv.heading("fin", text="Финиш")

        self.runs_tv.column("run", width=92, anchor="center")
        self.runs_tv.column("cat", width=120, anchor="center")
        self.runs_tv.column("start", width=140, anchor="center")
        self.runs_tv.column("ath", width=90, anchor="center")
        self.runs_tv.column("fin", width=80, anchor="center")

        self.runs_tv.tag_configure("odd", background=c["odd"])
        self.runs_tv.tag_configure("even", background=c["even"])

        rw = ttk.Frame(lpad, style="Card.TFrame")
        rw.pack(fill="both", expand=True)
        vsb = ttk.Scrollbar(rw, orient="vertical", command=self.runs_tv.yview)
        self.runs_tv.configure(yscrollcommand=vsb.set)
        self.runs_tv.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")
        self.runs_tv.bind("<<TreeviewSelect>>", self._on_run_select)

        rpad = ttk.Frame(right, style="Card.TFrame")
        rpad.pack(fill="both", expand=True, padx=12, pady=12)

        head2 = ttk.Frame(rpad, style="Card.TFrame")
        head2.pack(fill="x")
        ttk.Label(head2, text="Участники", style="H2.TLabel").pack(side="left")
        self.run_info_var = tk.StringVar(value="—")
        ttk.Label(head2, textvariable=self.run_info_var, style="Muted.TLabel").pack(side="right")

        catrow = ttk.Frame(rpad, style="Card.TFrame")
        catrow.pack(fill="x", pady=(10, 8))
        ttk.Label(catrow, text="Категория заезда", style="Muted.TLabel").pack(side="left")
        self.run_cat_cb = ttk.Combobox(catrow, textvariable=self.run_category_var, values=self.settings.categories, state="readonly", width=22)
        self.run_cat_cb.pack(side="left", padx=(8, 0))
        self.run_cat_cb.bind("<<ComboboxSelected>>", lambda _e: self._on_run_category_changed())

        ttk.Label(catrow, text="Фильтр", style="Muted.TLabel").pack(side="left", padx=(18, 0))
        ttk.Entry(catrow, textvariable=self.ath_filter_var, width=24).pack(side="left", padx=(8, 0))
        self.ath_filter_var.trace_add("write", lambda *_: self._render_athletes(self.selected_run_key))

        ttk.Button(catrow, text="Загрузить состав…", command=self._load_roster_for_current_category).pack(side="right")
        ttk.Button(catrow, text="Экспорт заезда…", command=self._export_selected_run).pack(side="right", padx=(0, 8))

        self.ath_container = ttk.Frame(rpad, style="Card.TFrame")
        self.ath_container.pack(fill="both", expand=True)

        self._ath_cols: List[str] = []
        self.ath_tv: Optional[ttk.Treeview] = None
        self._rebuild_ath_tree(split_ids=[])
        self.ath_tv.bind("<Double-1>", self._on_athlete_double_click)

    def _build_roster_tab(self):
        c = self.colors

        outer = ttk.Frame(self.tab_roster, style="TFrame")
        outer.pack(fill="both", expand=True, padx=12, pady=12)

        top = ttk.Frame(outer, style="Card.TFrame")
        top.pack(fill="x")
        top_in = ttk.Frame(top, style="Card.TFrame")
        top_in.pack(fill="x", padx=12, pady=12)

        ttk.Label(top_in, text="Категория", style="Muted.TLabel").pack(side="left")
        self.roster_cat_cb = ttk.Combobox(top_in, textvariable=self.category_var, values=self.settings.categories, state="readonly", width=22)
        self.roster_cat_cb.pack(side="left", padx=(8, 12))
        self.roster_cat_cb.bind("<<ComboboxSelected>>", lambda _e: self._render_roster())

        ttk.Button(top_in, text="Загрузить Excel/CSV…", style="Accent.TButton", command=self._load_roster_dialog).pack(side="left")
        ttk.Button(top_in, text="Очистить категорию", command=self._clear_roster_category).pack(side="left", padx=(8, 0))
        ttk.Button(top_in, text="Сохранить", command=self._save_roster).pack(side="left", padx=(8, 0))

        ttk.Button(top_in, text="Категории…", command=self._edit_categories).pack(side="right")

        body = ttk.Frame(outer, style="Card.TFrame")
        body.pack(fill="both", expand=True, pady=(12, 0))
        body_in = ttk.Frame(body, style="Card.TFrame")
        body_in.pack(fill="both", expand=True, padx=12, pady=12)

        cols = ("bib", "name", "country")
        self.roster_tv = ttk.Treeview(body_in, columns=cols, show="headings")
        self.roster_tv.heading("bib", text="№")
        self.roster_tv.heading("name", text="Имя")
        self.roster_tv.heading("country", text="Страна/Город")
        self.roster_tv.column("bib", width=90, anchor="center")
        self.roster_tv.column("name", width=520, anchor="w")
        self.roster_tv.column("country", width=180, anchor="center")

        self.roster_tv.tag_configure("odd", background=c["odd"])
        self.roster_tv.tag_configure("even", background=c["even"])

        vsb = ttk.Scrollbar(body_in, orient="vertical", command=self.roster_tv.yview)
        self.roster_tv.configure(yscrollcommand=vsb.set)
        self.roster_tv.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        self.roster_tv.bind("<Double-1>", self._on_roster_double_click)

        hint = ttk.Label(outer, text="Двойной клик по строке: редактирование записи в составе.", style="Muted.TLabel")
        hint.pack(anchor="w", pady=(10, 0))

        self._render_roster()

    def _build_export_tab(self):
        outer = ttk.Frame(self.tab_export, style="TFrame")
        outer.pack(fill="both", expand=True, padx=12, pady=12)

        card = ttk.Frame(outer, style="Card.TFrame")
        card.pack(fill="x")
        pad = ttk.Frame(card, style="Card.TFrame")
        pad.pack(fill="x", padx=12, pady=12)

        ttk.Label(pad, text="Экспорт", style="H2.TLabel").pack(anchor="w")

        row = ttk.Frame(pad, style="Card.TFrame")
        row.pack(fill="x", pady=(10, 0))
        ttk.Button(row, text="Экспорт выбранного заезда в CSV…", style="Accent.TButton", command=self._export_selected_run).pack(side="left")
        ttk.Button(row, text="Экспорт всех заездов в CSV…", command=self._export_all_runs).pack(side="left", padx=(8, 0))
        ttk.Button(row, text="Сохранить JSON-снимок…", command=self._save_snapshot_json).pack(side="left", padx=(8, 0))
        ttk.Button(row, text="Копировать выбранный заезд (текст)", command=self._copy_selected_run_text).pack(side="left", padx=(8, 0))

        self.export_text = tk.Text(outer, height=22, wrap="none",
                                   bg=self.colors["panel2"], fg=self.colors["fg"],
                                   insertbackground=self.colors["fg"],
                                   relief="flat", highlightthickness=1,
                                   highlightbackground=self.colors["line"], highlightcolor=self.colors["accent"])
        self.export_text.pack(fill="both", expand=True, pady=(12, 0))

        self._update_export_preview()

    def _build_log_tab(self):
        outer = ttk.Frame(self.tab_log, style="TFrame")
        outer.pack(fill="both", expand=True, padx=12, pady=12)

        card = ttk.Frame(outer, style="Card.TFrame")
        card.pack(fill="both", expand=True)
        pad = ttk.Frame(card, style="Card.TFrame")
        pad.pack(fill="both", expand=True, padx=12, pady=12)

        ttk.Label(pad, text="Лог (последние ~1500 строк)", style="H2.TLabel").pack(anchor="w", pady=(0, 10))
        self.log = tk.Text(pad, wrap="none",
                           bg=self.colors["panel2"], fg=self.colors["fg"],
                           insertbackground=self.colors["fg"],
                           relief="flat", highlightthickness=1,
                           highlightbackground=self.colors["line"], highlightcolor=self.colors["accent"])
        self.log.pack(fill="both", expand=True)

    def connect(self):
        if self._thr and self._thr.is_alive():
            return
        host = self.host_var.get().strip() or "127.0.0.1"
        try:
            port = int(self.port_var.get().strip())
        except Exception:
            messagebox.showerror("Ошибка", "Неверный port")
            return

        self._stop_evt.clear()
        self._thr = TcpClientThread(host, port, self._q, self._stop_evt)
        self._thr.start()

    def disconnect(self):
        self._stop_evt.set()

    def _set_status(self, text: str, ok: bool):
        self.status_var.set(text)
        color = self.colors["ok"] if ok else self.colors["danger"]
        try:
            self.status_dot.itemconfigure(self._dot_id, fill=color, outline=color)
        except Exception:
            pass
        self._connected = ok

    def _pump(self):
        try:
            while True:
                item = self._q.get_nowait()
                k = item.get("kind")
                if k == "status":
                    self._set_status(str(item.get("text", "")), bool(item.get("ok")))
                elif k == "error":
                    self._append_log("ERROR: " + str(item.get("text", "")))
                elif k == "raw":
                    self._append_log(str(item.get("text", "")))
                elif k == "msg":
                    raw = item.get("raw")
                    if raw:
                        self._append_log(raw)
                    self._handle_message(item.get("data") or {})
        except queue.Empty:
            pass

        if (not self._connected) and self.auto_reconnect_var.get():
            if (time.time() - self._last_state_ts) > 2.0:
                if not (self._thr and self._thr.is_alive()):
                    self._last_state_ts = time.time()
                    self.connect()

        self.after(80, self._pump)

    def _handle_message(self, msg: Dict[str, Any]):
        t = msg.get("type")
        if t == "hello":
            return
        if t == "state":
            st = msg.get("state")
            if isinstance(st, dict):
                self.state = st
                self._last_state_ts = time.time()
                self._refresh_views()
                return

    def _refresh_views(self):
        self._render_runs()
        self._render_athletes(self.selected_run_key)
        self._update_export_preview()

    def _render_runs(self):
        for iid in self.runs_tv.get_children():
            self.runs_tv.delete(iid)

        runs = self.state.get("runs", {}) if isinstance(self.state.get("runs"), dict) else {}
        keys = list(runs.keys())

        flt = self.run_filter_var.get().strip().lower()

        idx = 0
        for run_key in keys:
            run = runs.get(run_key) or {}
            start = run.get("start_time") or ""
            ath = run.get("athletes") or {}
            ath_n = len(ath) if isinstance(ath, dict) else 0
            fin_n = 0
            if isinstance(ath, dict):
                for _bib, a in ath.items():
                    if isinstance(a, dict) and a.get("finish") is not None:
                        fin_n += 1

            cat = self.settings.run_categories.get(run_key, "")
            hay = f"{run_key} {cat} {start} {ath_n} {fin_n}".lower()
            if flt and flt not in hay:
                continue

            tag = "even" if (idx % 2 == 0) else "odd"
            self.runs_tv.insert("", "end",
                                iid=run_key,
                                values=(run_key, cat, start, ath_n, fin_n),
                                tags=(tag,))
            idx += 1

        cur = self.selected_run_key or self.state.get("current_key")
        if cur and cur in self.runs_tv.get_children():
            try:
                self.runs_tv.selection_set(cur)
                self.runs_tv.see(cur)
            except Exception:
                pass

    def _on_run_select(self, _evt=None):
        sel = self.runs_tv.selection()
        if not sel:
            self.selected_run_key = None
            self.run_category_var.set("")
            self._render_athletes(None)
            return
        run_key = sel[0]
        self.selected_run_key = run_key
        cat = self.settings.run_categories.get(run_key, "")
        if cat:
            self.run_category_var.set(cat)
        else:
            if self.category_var.get().strip():
                self.run_category_var.set(self.category_var.get().strip())
        self._render_athletes(run_key)
        self._update_export_preview()

    def _on_run_category_changed(self):
        run_key = self.selected_run_key
        if not run_key:
            return
        cat = self.run_category_var.get().strip()
        if not cat:
            return
        self.settings.run_categories[run_key] = cat
        self.category_var.set(cat)
        self._render_runs()
        self._render_athletes(run_key)
        self._render_roster()
        self._update_export_preview()

    def _run_split_ids(self, run: Dict[str, Any]) -> List[str]:
        ath = run.get("athletes")
        ids = set()
        if isinstance(ath, dict):
            for _bib, a in ath.items():
                if not isinstance(a, dict):
                    continue
                sp = a.get("splits")
                if isinstance(sp, dict):
                    ids.update(sp.keys())
        return sorted(ids, key=split_sort_key)

    def _rebuild_ath_tree(self, split_ids: List[str]):
        for child in self.ath_container.winfo_children():
            child.destroy()

        cols = ["bib", "name", "country"]
        if self.show_distance_var.get():
            cols.append("dist")
        cols += [f"S{sid}" for sid in split_ids] + ["finish", "status"]

        self._ath_cols = cols
        self.ath_tv = ttk.Treeview(self.ath_container, columns=cols, show="headings")
        vsb = ttk.Scrollbar(self.ath_container, orient="vertical", command=self.ath_tv.yview)
        hsb = ttk.Scrollbar(self.ath_container, orient="horizontal", command=self.ath_tv.xview)
        self.ath_tv.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.ath_tv.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        self.ath_container.rowconfigure(0, weight=1)
        self.ath_container.columnconfigure(0, weight=1)

        self.ath_tv.heading("bib", text="№")
        self.ath_tv.column("bib", width=90, anchor="center")
        self.ath_tv.heading("name", text="Имя")
        self.ath_tv.column("name", width=360, anchor="w")
        self.ath_tv.heading("country", text="Стр/Гор")
        self.ath_tv.column("country", width=120, anchor="center")

        if self.show_distance_var.get():
            self.ath_tv.heading("dist", text="Дист.")
            self.ath_tv.column("dist", width=90, anchor="center")

        for sid in split_ids:
            cid = f"S{sid}"
            self.ath_tv.heading(cid, text=f"S{sid}")
            self.ath_tv.column(cid, width=125, anchor="center")

        self.ath_tv.heading("finish", text="Финиш")
        self.ath_tv.column("finish", width=140, anchor="center")
        self.ath_tv.heading("status", text="Статус")
        self.ath_tv.column("status", width=160, anchor="center")

        self.ath_tv.tag_configure("odd", background=self.colors["odd"])
        self.ath_tv.tag_configure("even", background=self.colors["even"])

    def _effective_meta(self, run_key: str, athlete: Dict[str, Any]) -> Tuple[str, str]:
        bib = safe_int_str(athlete.get("bib") or "")
        if not bib:
            return "", ""
        srv_name = str(athlete.get("name") or "").strip()
        srv_country = str(athlete.get("country") or "").strip().upper()

        cat = self.settings.run_categories.get(run_key, "") or self.run_category_var.get().strip() or self.category_var.get().strip()
        if not cat:
            return srv_name, srv_country

        entry = self.roster.get_entry(cat, bib)
        if not entry:
            return srv_name, srv_country

        if self.override_names_var.get() or not srv_name:
            name = entry.get("name", "") or srv_name
        else:
            name = srv_name

        if self.override_names_var.get() or not srv_country:
            country = entry.get("country", "") or srv_country
        else:
            country = srv_country

        return name, country

    def _render_athletes(self, run_key: Optional[str]):
        if not self.ath_tv:
            return
        for iid in self.ath_tv.get_children():
            self.ath_tv.delete(iid)

        if not run_key:
            self.run_info_var.set("—")
            return

        runs = self.state.get("runs", {}) if isinstance(self.state.get("runs"), dict) else {}
        run = runs.get(run_key) if isinstance(runs.get(run_key), dict) else None
        if not run:
            self.run_info_var.set("—")
            return

        split_ids = self._run_split_ids(run)
        desired_cols = ["bib", "name", "country"]
        if self.show_distance_var.get():
            desired_cols.append("dist")
        desired_cols += [f"S{sid}" for sid in split_ids] + ["finish", "status"]
        if desired_cols != self._ath_cols:
            self._rebuild_ath_tree(split_ids=split_ids)
            self.ath_tv.bind("<Double-1>", self._on_athlete_double_click)

        start = run.get("start_time") or "—"
        ath = run.get("athletes") or {}
        ath_n = len(ath) if isinstance(ath, dict) else 0
        fin_n = 0
        if isinstance(ath, dict):
            for _bib, a in ath.items():
                if isinstance(a, dict) and a.get("finish") is not None:
                    fin_n += 1

        cat = self.settings.run_categories.get(run_key, "") or "—"
        self.run_info_var.set(f"{run_key}   кат: {cat}   старт: {start}   участников: {ath_n}   финиш: {fin_n}")

        flt = self.ath_filter_var.get().strip().lower()

        order = run.get("bib_order")
        if isinstance(order, list) and order:
            bibs = [safe_int_str(b) for b in order if safe_int_str(b)]
        else:
            bibs = [safe_int_str(b) for b in (ath.keys() if isinstance(ath, dict) else [])]
        bibs = [b for b in bibs if b in ath]

        idx = 0
        for bib in bibs:
            a = ath.get(bib)
            if not isinstance(a, dict):
                continue

            eff_name, eff_country = self._effective_meta(run_key, a)

            splits = a.get("splits") or {}
            finish = a.get("finish")
            status = a.get("status") or ""

            dist = ""
            if self.show_distance_var.get():
                split_count = len(splits) if isinstance(splits, dict) else 0
                dist = f"{split_count * DIST_PER_SPLIT_M}м" if split_count > 0 else ""

            if flt:
                hay = f"{bib} {eff_name} {eff_country} {status}".lower()
                if flt not in hay:
                    continue

            row = [bib, eff_name, eff_country]
            if self.show_distance_var.get():
                row.append(dist)

            for sid in split_ids:
                v = ""
                if isinstance(splits, dict):
                    v = fmt_time(splits.get(str(sid)))
                row.append(v)

            row.append(fmt_time(finish))
            row.append(str(status))

            tag = "even" if (idx % 2 == 0) else "odd"
            self.ath_tv.insert("", "end", iid=f"{run_key}:{bib}", values=row, tags=(tag,))
            idx += 1

    def _render_roster(self):
        for iid in self.roster_tv.get_children():
            self.roster_tv.delete(iid)

        cat = self.category_var.get().strip()
        if not cat:
            return

        mp = self.roster.data.get(cat, {})
        bibs = sorted(mp.keys(), key=split_sort_key)

        idx = 0
        for bib in bibs:
            meta = mp.get(bib) or {}
            name = meta.get("name", "")
            country = meta.get("country", "")
            tag = "even" if (idx % 2 == 0) else "odd"
            self.roster_tv.insert("", "end", iid=f"{cat}:{bib}", values=(bib, name, country), tags=(tag,))
            idx += 1

    def _save_roster(self):
        self.roster.save()
        self._append_log("ROSTER: saved")

    def _clear_roster_category(self):
        cat = self.category_var.get().strip()
        if not cat:
            return
        if messagebox.askyesno("Очистить", f"Очистить состав категории: {cat}?"):
            self.roster.clear_category(cat)
            self.roster.save()
            self._render_roster()
            self._refresh_views()

    def _edit_roster_entry(self, cat: str, bib: str, name: str, country: str) -> bool:
        d = tk.Toplevel(self)
        d.title("Запись состава")
        d.resizable(False, False)

        v_bib = tk.StringVar(value=bib)
        v_name = tk.StringVar(value=name)
        v_country = tk.StringVar(value=country)

        frm = ttk.Frame(d)
        frm.pack(padx=12, pady=12, fill="x")

        ttk.Label(frm, text=f"Категория: {cat}", style="Muted.TLabel").grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 8))

        ttk.Label(frm, text="№").grid(row=1, column=0, sticky="w")
        ttk.Entry(frm, textvariable=v_bib, width=10).grid(row=1, column=1, sticky="w")

        ttk.Label(frm, text="Имя").grid(row=2, column=0, sticky="w", pady=(8, 0))
        e_n = ttk.Entry(frm, textvariable=v_name, width=42)
        e_n.grid(row=2, column=1, sticky="w", pady=(8, 0))

        ttk.Label(frm, text="Страна/Город").grid(row=3, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(frm, textvariable=v_country, width=14).grid(row=3, column=1, sticky="w", pady=(8, 0))

        out = {"ok": False}

        def ok():
            bb = safe_int_str(v_bib.get())
            if not bb:
                messagebox.showerror("Ошибка", "Неверный номер")
                return
            self.roster.set_entry(cat, bb, v_name.get(), v_country.get())
            self.roster.save()
            out["ok"] = True
            d.destroy()

        def delete():
            bb = safe_int_str(v_bib.get())
            if not bb:
                return
            if messagebox.askyesno("Удалить", f"Удалить запись №{bb} из {cat}?"):
                self.roster.delete_entry(cat, bb)
                self.roster.save()
                out["ok"] = True
                d.destroy()

        def cancel():
            d.destroy()

        btns = ttk.Frame(d)
        btns.pack(padx=12, pady=(0, 12), fill="x")
        ttk.Button(btns, text="Отмена", command=cancel).pack(side="right")
        ttk.Button(btns, text="OK", style="Accent.TButton", command=ok).pack(side="right", padx=(0, 8))
        ttk.Button(btns, text="Удалить", command=delete).pack(side="left")

        d.grab_set()
        e_n.focus_set()
        d.wait_window()
        return out["ok"]

    def _on_roster_double_click(self, _evt=None):
        sel = self.roster_tv.selection()
        if not sel:
            return
        iid = sel[0]
        try:
            cat, bib = iid.split(":", 1)
        except Exception:
            return
        mp = self.roster.data.get(cat, {})
        meta = mp.get(bib, {})
        if self._edit_roster_entry(cat, bib, meta.get("name", ""), meta.get("country", "")):
            self._render_roster()
            self._refresh_views()

    def _on_athlete_double_click(self, _evt=None):
        if not self.ath_tv:
            return
        sel = self.ath_tv.selection()
        if not sel:
            return
        iid = sel[0]
        try:
            run_key, bib = iid.split(":", 1)
        except Exception:
            return
        cat = self.settings.run_categories.get(run_key, "") or self.run_category_var.get().strip() or self.category_var.get().strip()
        if not cat:
            messagebox.showerror("Ошибка", "Сначала назначь категорию заезда")
            return
        name, country = "", ""
        entry = self.roster.get_entry(cat, bib)
        if entry:
            name, country = entry.get("name", ""), entry.get("country", "")
        else:
            runs = self.state.get("runs", {}) if isinstance(self.state.get("runs"), dict) else {}
            run = runs.get(run_key, {}) if isinstance(runs.get(run_key), dict) else {}
            ath = run.get("athletes") or {}
            a = ath.get(bib) if isinstance(ath, dict) else None
            if isinstance(a, dict):
                name = str(a.get("name") or "").strip()
                country = str(a.get("country") or "").strip().upper()

        if self._edit_roster_entry(cat, bib, name, country):
            self._render_roster()
            self._refresh_views()

    def _load_roster_for_current_category(self):
        cat = self.run_category_var.get().strip() or self.category_var.get().strip()
        if not cat:
            messagebox.showerror("Ошибка", "Сначала выбери категорию")
            return
        self.category_var.set(cat)
        self._load_roster_dialog(forced_category=cat)

    def _load_roster_dialog(self, forced_category: Optional[str] = None):
        ft = [("Excel", "*.xlsx")] if load_workbook else []
        ft += [("CSV", "*.csv"), ("All", "*.*")]
        path = filedialog.askopenfilename(title="Загрузить состав", filetypes=ft)
        if not path:
            return
        try:
            loaded, cats = self._load_roster_file(path, forced_category=forced_category)
            self.roster.save()
            self._append_log(f"ROSTER: loaded {loaded} records")
            if cats:
                self._append_log("ROSTER categories: " + ", ".join(cats))
            self._render_roster()
            self._refresh_views()
        except Exception as e:
            messagebox.showerror("Ошибка", str(e))

    def _load_roster_file(self, path: str, forced_category: Optional[str]) -> Tuple[int, List[str]]:
        p = Path(path)
        ext = p.suffix.lower()
        if ext == ".xlsx":
            if load_workbook is None:
                raise RuntimeError("openpyxl не установлен")
            return self._load_roster_xlsx(p, forced_category)
        return self._load_roster_csv(p, forced_category)

    def _load_roster_xlsx(self, p: Path, forced_category: Optional[str]) -> Tuple[int, List[str]]:
        wb = load_workbook(str(p), data_only=True)
        ws = wb.active

        cat_default = forced_category or self.category_var.get().strip()
        if not cat_default:
            cat_default = self.settings.categories[0] if self.settings.categories else "Категория"

        header = [str(x).strip().lower() if x is not None else "" for x in next(ws.iter_rows(min_row=1, max_row=1, values_only=True))]
        has_header = any(h in ("bib", "номер", "№", "name", "имя", "country", "страна", "категория", "category") for h in header)
        start_row = 2 if has_header else 1

        def idx_of(names: List[str]) -> Optional[int]:
            for i, h in enumerate(header):
                if h in names:
                    return i
            return None

        i_bib = idx_of(["bib", "номер", "№"]) or 0
        i_name = idx_of(["name", "имя", "фио"]) or 1
        i_country = idx_of(["country", "страна", "город"]) or 2
        i_cat = idx_of(["category", "категория"])

        touched = set()
        n = 0
        for r in ws.iter_rows(min_row=start_row, values_only=True):
            if not r:
                continue
            bib = safe_int_str(r[i_bib] if i_bib < len(r) else "")
            if not bib:
                continue
            name = str(r[i_name]).strip() if i_name < len(r) and r[i_name] is not None else ""
            country = str(r[i_country]).strip().upper() if i_country < len(r) and r[i_country] is not None else ""
            cat = None
            if i_cat is not None and i_cat < len(r) and r[i_cat] is not None:
                cat = str(r[i_cat]).strip()
            if not cat:
                cat = cat_default
            self.roster.set_entry(cat, bib, name, country)
            touched.add(cat)
            n += 1

        return n, sorted(touched)

    def _load_roster_csv(self, p: Path, forced_category: Optional[str]) -> Tuple[int, List[str]]:
        cat_default = forced_category or self.category_var.get().strip()
        if not cat_default:
            cat_default = self.settings.categories[0] if self.settings.categories else "Категория"

        touched = set()
        n = 0

        text = p.read_text(encoding="utf-8", errors="ignore")
        first_line = text.splitlines()[0] if text.splitlines() else ""
        delim = ";" if first_line.count(";") >= first_line.count(",") else ","

        rows = list(csv.reader(text.splitlines(), delimiter=delim))
        if not rows:
            return 0, []

        header = [str(x).strip().lower() for x in rows[0]]
        has_header = any(h in ("bib", "номер", "№", "name", "имя", "фио", "country", "страна", "город", "category", "категория") for h in header)

        def idx_of(names: List[str]) -> Optional[int]:
            for i, h in enumerate(header):
                if h in names:
                    return i
            return None

        i_bib = idx_of(["bib", "номер", "№"]) if has_header else 0
        i_name = idx_of(["name", "имя", "фио"]) if has_header else 1
        i_country = idx_of(["country", "страна", "город"]) if has_header else 2
        i_cat = idx_of(["category", "категория"]) if has_header else None

        start = 1 if has_header else 0

        for r in rows[start:]:
            if not r:
                continue
            bib = safe_int_str(r[i_bib] if i_bib is not None and i_bib < len(r) else "")
            if not bib:
                continue
            name = str(r[i_name]).strip() if i_name is not None and i_name < len(r) else ""
            country = str(r[i_country]).strip().upper() if i_country is not None and i_country < len(r) else ""
            cat = ""
            if i_cat is not None and i_cat < len(r):
                cat = str(r[i_cat]).strip()
            if not cat:
                cat = cat_default
            self.roster.set_entry(cat, bib, name, country)
            touched.add(cat)
            n += 1

        return n, sorted(touched)

    def _selected_run(self) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        run_key = self.selected_run_key
        if not run_key:
            run_key = self.state.get("current_key")
        if not run_key:
            return None, None
        runs = self.state.get("runs", {}) if isinstance(self.state.get("runs"), dict) else {}
        run = runs.get(run_key) if isinstance(runs.get(run_key), dict) else None
        return run_key, run

    def _export_selected_run(self):
        run_key, run = self._selected_run()
        if not run_key or not run:
            messagebox.showerror("Ошибка", "Нет выбранного заезда")
            return
        path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV", "*.csv")],
                                            initialfile=f"run_{run_key}.csv")
        if not path:
            return
        self._export_runs_to_csv(path, only_run_key=run_key)
        messagebox.showinfo("Готово", f"Сохранено: {path}")

    def _export_all_runs(self):
        if not self.state.get("runs"):
            messagebox.showerror("Ошибка", "Нет данных")
            return
        path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV", "*.csv")],
                                            initialfile="all_runs.csv")
        if not path:
            return
        self._export_runs_to_csv(path, only_run_key=None)
        messagebox.showinfo("Готово", f"Сохранено: {path}")

    def _export_runs_to_csv(self, path: str, only_run_key: Optional[str]):
        runs = self.state.get("runs", {}) if isinstance(self.state.get("runs"), dict) else {}
        keys = [only_run_key] if only_run_key else list(runs.keys())

        all_splits = set()
        for k in keys:
            run = runs.get(k) or {}
            for sid in self._run_split_ids(run):
                all_splits.add(str(sid))
        all_splits_sorted = sorted(all_splits, key=split_sort_key)

        cols = ["run", "category", "bib", "name", "country"]
        if self.show_distance_var.get():
            cols.append("distance_m")
        cols += [f"S{sid}" for sid in all_splits_sorted] + ["finish", "status"]

        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(cols)
            for run_key in keys:
                run = runs.get(run_key) or {}
                cat = self.settings.run_categories.get(run_key, "")
                ath = run.get("athletes") or {}
                order = run.get("bib_order")
                if isinstance(order, list) and order:
                    bibs = [safe_int_str(b) for b in order if safe_int_str(b)]
                else:
                    bibs = [safe_int_str(b) for b in (ath.keys() if isinstance(ath, dict) else [])]
                bibs = [b for b in bibs if b in ath]

                for bib in bibs:
                    a = ath.get(bib)
                    if not isinstance(a, dict):
                        continue
                    name, country = self._effective_meta(run_key, a)
                    splits = a.get("splits") or {}
                    finish = a.get("finish")
                    status = a.get("status") or ""
                    row = [run_key, cat, bib, name, country]
                    if self.show_distance_var.get():
                        split_count = len(splits) if isinstance(splits, dict) else 0
                        row.append(split_count * DIST_PER_SPLIT_M if split_count > 0 else "")
                    for sid in all_splits_sorted:
                        v = ""
                        if isinstance(splits, dict):
                            v = fmt_time(splits.get(str(sid)))
                        row.append(v)
                    row.append(fmt_time(finish))
                    row.append(status)
                    w.writerow(row)

    def _save_snapshot_json(self):
        if not self.state:
            messagebox.showerror("Ошибка", "Нет данных")
            return
        path = filedialog.asksaveasfilename(defaultextension=".json", filetypes=[("JSON", "*.json")],
                                            initialfile=f"snapshot_{int(now_ts())}.json")
        if not path:
            return
        snap = {
            "ts_saved": now_ts(),
            "run_categories": self.settings.run_categories,
            "state": self.state,
        }
        Path(path).write_text(json.dumps(snap, ensure_ascii=False, indent=2), encoding="utf-8")
        messagebox.showinfo("Готово", f"Сохранено: {path}")

    def _copy_selected_run_text(self):
        run_key, run = self._selected_run()
        if not run_key or not run:
            return
        txt = self._build_run_text(run_key, run)
        try:
            self.clipboard_clear()
            self.clipboard_append(txt)
        except Exception:
            pass
        self.export_text.delete("1.0", "end")
        self.export_text.insert("1.0", txt)

    def _build_run_text(self, run_key: str, run: Dict[str, Any]) -> str:
        cat = self.settings.run_categories.get(run_key, "")
        start = run.get("start_time") or ""
        lines = []
        lines.append(f"Заезд: {run_key}")
        if cat:
            lines.append(f"Категория: {cat}")
        if start:
            lines.append(f"Старт: {start}")
        lines.append("")
        lines.append("№;Имя;Стр/Гор;Отсечки;Финиш;Статус")

        split_ids = self._run_split_ids(run)
        ath = run.get("athletes") or {}
        order = run.get("bib_order")
        if isinstance(order, list) and order:
            bibs = [safe_int_str(b) for b in order if safe_int_str(b)]
        else:
            bibs = [safe_int_str(b) for b in (ath.keys() if isinstance(ath, dict) else [])]
        bibs = [b for b in bibs if b in ath]

        for bib in bibs:
            a = ath.get(bib)
            if not isinstance(a, dict):
                continue
            name, country = self._effective_meta(run_key, a)
            splits = a.get("splits") or {}
            sp = []
            for sid in split_ids:
                if isinstance(splits, dict) and str(sid) in splits:
                    sp.append(f"S{sid}:{fmt_time(splits.get(str(sid)))}")
            finish = fmt_time(a.get("finish"))
            status = str(a.get("status") or "")
            lines.append(f"{bib};{name};{country};{' '.join(sp)};{finish};{status}")
        return "\n".join(lines) + "\n"

    def _update_export_preview(self):
        run_key, run = self._selected_run()
        if not run_key or not run:
            txt = "Нет выбранного заезда.\n"
        else:
            txt = self._build_run_text(run_key, run)
        try:
            cur = self.export_text.get("1.0", "end")
            if cur != txt:
                self.export_text.delete("1.0", "end")
                self.export_text.insert("1.0", txt)
        except Exception:
            pass

    def _edit_categories(self):
        d = CategoryDialog(self, self.settings.categories)
        d.wait_window()
        if not d.result:
            return
        old = list(self.settings.categories)
        new = list(d.result)

        self.settings.categories = new
        self.run_cat_cb["values"] = new
        self.roster_cat_cb["values"] = new

        if self.category_var.get() not in new:
            self.category_var.set(new[0])
        if self.run_category_var.get() not in new and self.run_category_var.get().strip():
            self.run_category_var.set(new[0])

        for c in new:
            self.roster.ensure_category(c)

        removed = set(old) - set(new)
        if removed:
            for rk, cat in list(self.settings.run_categories.items()):
                if cat in removed:
                    self.settings.run_categories[rk] = ""

        self._render_roster()
        self._refresh_views()

    def _append_log(self, s: str):
        s = str(s).rstrip()
        if not s:
            return
        try:
            self.log.insert("end", s + "\n")
            lines = int(self.log.index("end-1c").split(".")[0])
            if lines > 1500:
                self.log.delete("1.0", f"{lines-1500}.0")
            self.log.see("end")
        except Exception:
            pass

    def _auto_save_tick(self):
        self._save_settings()
        self.after(600, self._auto_save_tick)

    def _save_settings(self):
        try:
            self.settings.host = self.host_var.get().strip() or self.settings.host
            try:
                self.settings.port = int(self.port_var.get().strip())
            except Exception:
                pass
            self.settings.auto_reconnect = bool(self.auto_reconnect_var.get())
            self.settings.override_server_names = bool(self.override_names_var.get())
            self.settings.show_distance = bool(self.show_distance_var.get())
            self.settings_store.save(self.settings)
        except Exception:
            pass

    def _on_close(self):
        self._save_settings()
        try:
            self.disconnect()
        except Exception:
            pass
        self.destroy()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=None)
    ap.add_argument("--port", type=int, default=None)
    args = ap.parse_args()

    app = App()
    if args.host:
        app.host_var.set(args.host)
    if args.port:
        app.port_var.set(str(args.port))

    app.mainloop()


if __name__ == "__main__":
    main()
