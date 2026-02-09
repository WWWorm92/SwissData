# quantum_client_table.py
# клиент: одна таблица, заезды группами (заголовок-строка), внутри: №, имя, отсечки, финиш

import argparse
import json
import queue
import socket
import threading
import tkinter as tk
from tkinter import ttk, messagebox
from typing import Dict, Any, Optional, List


def split_sort_key(x: str):
    x = str(x).strip()
    if x.isdigit():
        return (0, int(x))
    return (1, x)


def fmt_time(sec):
    if sec is None:
        return ""
    try:
        sec = float(sec)
    except Exception:
        return ""
    total_ms = int(round(sec * 1000))
    s = (total_ms // 1000) % 60
    m = (total_ms // 60000) % 60
    h = total_ms // 3600000
    ms = total_ms % 1000
    if h > 0:
        return f"{h:d}:{m:02d}:{s:02d}.{ms:03d}"
    if total_ms >= 60000:
        return f"{m:d}:{s:02d}.{ms:03d}"
    return f"{total_ms/1000:.3f}"


def safe_col_id(x: str) -> str:
    x = str(x)
    out = []
    for ch in x:
        out.append(ch if ch.isalnum() else "_")
    return "s_" + "".join(out)


class NetThread(threading.Thread):
    def __init__(self, q: queue.Queue, stop_evt: threading.Event, host: str, port: int):
        super().__init__(daemon=True)
        self.q = q
        self.stop_evt = stop_evt
        self.host = host
        self.port = port
        self.sock: Optional[socket.socket] = None

    def run(self):
        buf = b""
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(2.0)
            self.sock.connect((self.host, self.port))
            self.q.put(("status", "connected"))
            self.sock.settimeout(0.5)

            while not self.stop_evt.is_set():
                try:
                    chunk = self.sock.recv(4096)
                    if chunk == b"":
                        raise RuntimeError("server disconnected")
                    buf += chunk

                    while b"\n" in buf:
                        line, buf = buf.split(b"\n", 1)
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            obj = json.loads(line.decode("utf-8", errors="ignore"))
                        except Exception:
                            continue
                        if obj.get("type") == "state" and isinstance(obj.get("state"), dict):
                            self.q.put(("state", obj["state"]))
                except socket.timeout:
                    continue

        except Exception as e:
            self.q.put(("err", str(e)))
        finally:
            try:
                if self.sock:
                    self.sock.close()
            except Exception:
                pass


class App(tk.Tk):
    def __init__(self, host: str, port: int):
        super().__init__()
        self.title("Quantum Client")
        self.geometry("1400x780")
        self.minsize(1100, 620)

        self.q = queue.Queue()
        self.stop_evt = threading.Event()
        self.net: Optional[NetThread] = None

        self.state: Dict[str, Any] = {}
        self.split_ids: List[str] = []

        self.host_var = tk.StringVar(value=host)
        self.port_var = tk.StringVar(value=str(port))
        self.status_var = tk.StringVar(value="Отключено")
        self.cat_var = tk.StringVar(value="ALL")
        self._cat_values = ["ALL"]

        self._colors = {
            "bg": "#0f1117",
            "panel": "#151a23",
            "panel2": "#111621",
            "fg": "#e6e6e6",
            "muted": "#a8b0bf",
            "line": "#242b3a",
            "accent": "#3aa0ff",
            "select": "#243044",
            "head": "#1b2230",
            "odd": "#121826",
            "even": "#0f1522",
        }

        self._setup_style()
        self._build_ui()
        self.after(50, self._pump)

    def _refresh_categories(self):
        runs = (self.state or {}).get("runs") or {}
        cats = sorted({(v.get("category") or "DEFAULT") for v in runs.values() if isinstance(v, dict)})
        values = ["ALL"] + cats
        if values != getattr(self, "_cat_values", ["ALL"]):
            self._cat_values = values
            try:
                self.cat_cb["values"] = values
            except Exception:
                pass
        cur = (self.cat_var.get() or "ALL")
        if cur not in values:
            self.cat_var.set("ALL")

    def _setup_style(self):
        c = self._colors
        self.configure(bg=c["bg"])
        style = ttk.Style()
        style.theme_use("clam")

        try:
            import tkinter.font as tkfont
            f = tkfont.nametofont("TkDefaultFont")
            f.configure(family="Segoe UI", size=12)
        except Exception:
            pass

        style.configure(".", background=c["bg"], foreground=c["fg"])
        style.configure("Card.TFrame", background=c["panel"])
        style.configure("TLabel", background=c["bg"], foreground=c["fg"])
        style.configure("Muted.TLabel", background=c["bg"], foreground=c["muted"])

        style.configure("TButton", background=c["panel"], foreground=c["fg"], padding=(14, 10), borderwidth=0)
        style.map("TButton", background=[("active", c["head"]), ("pressed", c["select"])])

        style.configure("Accent.TButton", background=c["accent"], foreground="#0b0d12", padding=(14, 10))
        style.map("Accent.TButton", background=[("active", "#5bb3ff"), ("pressed", "#2f8fe6")])

        style.configure("Treeview",
                        background=c["panel2"],
                        fieldbackground=c["panel2"],
                        foreground=c["fg"],
                        rowheight=38,
                        borderwidth=0)
        style.map("Treeview",
                  background=[("selected", c["select"])],
                  foreground=[("selected", c["fg"])])

        style.configure("Treeview.Heading",
                        background=c["head"],
                        foreground=c["fg"],
                        relief="flat",
                        padding=(10, 10),
                        borderwidth=0,
                        font=("Segoe UI", 12, "bold"))

    def _build_ui(self):
        c = self._colors

        bar = ttk.Frame(self, style="Card.TFrame")
        bar.pack(fill="x", padx=12, pady=12)
        inner = ttk.Frame(bar, style="Card.TFrame")
        inner.pack(fill="x", padx=12, pady=12)

        ttk.Label(inner, text="Server", style="Muted.TLabel").pack(side="left")

        host_entry = tk.Entry(inner, textvariable=self.host_var, bg=c["panel2"], fg=c["fg"],
                              insertbackground=c["fg"], relief="flat", highlightthickness=1,
                              highlightbackground=c["line"], highlightcolor=c["accent"])
        host_entry.pack(side="left", padx=(8, 10), ipady=6)

        ttk.Label(inner, text="Port", style="Muted.TLabel").pack(side="left")

        port_entry = tk.Entry(inner, textvariable=self.port_var, width=7, bg=c["panel2"], fg=c["fg"],
                              insertbackground=c["fg"], relief="flat", highlightthickness=1,
                              highlightbackground=c["line"], highlightcolor=c["accent"])
        port_entry.pack(side="left", padx=(8, 14), ipady=6)
        ttk.Label(inner, text="Категория:", style="Muted.TLabel").pack(side="left")
        self.cat_cb = ttk.Combobox(inner, textvariable=self.cat_var, width=14, values=self._cat_values,
                                   state="readonly")
        self.cat_cb.pack(side="left", padx=(8, 14), ipady=6)
        self.cat_var.trace_add("write", lambda *_: self._render_table())

        ttk.Button(inner, text="Connect", style="Accent.TButton", command=self.connect).pack(side="left", padx=(0, 10))
        ttk.Button(inner, text="Disconnect", command=self.disconnect).pack(side="left")

        ttk.Label(inner, textvariable=self.status_var, style="Muted.TLabel").pack(side="right")

        body = ttk.Frame(self, style="Card.TFrame")
        body.pack(fill="both", expand=True, padx=12, pady=(0, 12))

        body_inner = ttk.Frame(body, style="Card.TFrame")
        body_inner.pack(fill="both", expand=True, padx=12, pady=12)

        self.table_wrap = ttk.Frame(body_inner, style="Card.TFrame")
        self.table_wrap.pack(fill="both", expand=True)

        self._rebuild_table(split_ids=[])

    def _rebuild_table(self, split_ids: List[str]):
        for w in self.table_wrap.winfo_children():
            w.destroy()

        self.split_ids = list(split_ids)

        cols = ["bib", "name"] + [safe_col_id(s) for s in split_ids] + ["finish"]
        self.tv = ttk.Treeview(self.table_wrap, columns=cols, show="tree headings")

        vsb = ttk.Scrollbar(self.table_wrap, orient="vertical", command=self.tv.yview)
        hsb = ttk.Scrollbar(self.table_wrap, orient="horizontal", command=self.tv.xview)
        self.tv.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tv.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        self.table_wrap.rowconfigure(0, weight=1)
        self.table_wrap.columnconfigure(0, weight=1)

        self.tv.heading("#0", text="Заезд")
        self.tv.column("#0", width=130, anchor="center")

        self.tv.heading("bib", text="№")
        self.tv.column("bib", width=90, anchor="center")

        self.tv.heading("name", text="Имя")
        self.tv.column("name", width=260, anchor="w")

        for s in split_ids:
            cid = safe_col_id(s)
            self.tv.heading(cid, text=f"S{s}")
            self.tv.column(cid, width=130, anchor="center")

        self.tv.heading("finish", text="Финиш")
        self.tv.column("finish", width=150, anchor="center")

        self.tv.tag_configure("run_header", background=self._colors["head"], foreground=self._colors["fg"])
        self.tv.tag_configure("odd", background=self._colors["odd"])
        self.tv.tag_configure("even", background=self._colors["even"])

    def connect(self):
        if self.net and self.net.is_alive():
            return
        host = self.host_var.get().strip()
        if not host:
            messagebox.showerror("Ошибка", "Host пустой")
            return
        try:
            port = int(self.port_var.get().strip())
        except Exception:
            messagebox.showerror("Ошибка", "Port неверный")
            return

        self.stop_evt.clear()
        self.net = NetThread(self.q, self.stop_evt, host, port)
        self.net.start()
        self.status_var.set(f"Подключение к {host}:{port}…")

    def disconnect(self):
        self.stop_evt.set()
        self.status_var.set("Отключено")

    def _collect_all_splits(self, runs: Dict[str, Any]) -> List[str]:
        split_set = set()
        for run in runs.values():
            athletes = run.get("athletes") or {}
            for a in athletes.values():
                sp = a.get("splits") or {}
                split_set.update(sp.keys())
        return sorted(split_set, key=split_sort_key)

    def _run_keys_in_order(self, runs: Dict[str, Any]) -> List[str]:
        return list(runs.keys())

    def _render_table(self):
        runs = self.state.get("runs") or {}
        cat = (self.cat_var.get() or "").strip()
        if cat and cat.upper() != "ALL":
            runs = {k: v for k, v in runs.items() if isinstance(v, dict) and (v.get("category") or "DEFAULT") == cat}

        if not isinstance(runs, dict):
            runs = {}

        desired_splits = self._collect_all_splits(runs)
        if desired_splits != self.split_ids:
            self._rebuild_table(desired_splits)

        for iid in self.tv.get_children():
            self.tv.delete(iid)

        idx = 0
        for run_key in self._run_keys_in_order(runs):
            run = runs.get(run_key) or {}
            athletes = run.get("athletes") or {}

            header_values = [""] * (2 + len(self.split_ids) + 1)
            run_iid = self.tv.insert("", "end", text=str(run_key), values=tuple(header_values), tags=("run_header",))
            self.tv.item(run_iid, open=True)

            order = run.get("bib_order")
            bib_list: List[str] = []

            if isinstance(order, list) and order:
                seen = set()
                for b in order:
                    b = str(b).strip()
                    if b and b in athletes and b not in seen:
                        bib_list.append(b)
                        seen.add(b)
                for b in athletes.keys():
                    b = str(b).strip()
                    if b and b not in seen:
                        bib_list.append(b)
                        seen.add(b)
            else:
                bib_list = [str(b).strip() for b in athletes.keys() if str(b).strip()]

            for bib in bib_list:
                a = athletes.get(bib) or {}
                splits = a.get("splits") or {}
                row = [bib, (a.get("name") or "")]
                for s in self.split_ids:
                    row.append(fmt_time(splits.get(str(s))))
                row.append(fmt_time(a.get("finish")))

                tag = "even" if idx % 2 == 0 else "odd"
                self.tv.insert(run_iid, "end", text="", values=tuple(row), tags=(tag,))
                idx += 1

    def _pump(self):
        try:
            while True:
                kind, data = self.q.get_nowait()
                if kind == "status":
                    self.status_var.set("Подключено")
                elif kind == "state":
                    self.state = data
                    self._refresh_categories()
                    self._render_table()
                elif kind == "err":
                    self.status_var.set("Ошибка: " + str(data))
        except queue.Empty:
            pass
        self.after(50, self._pump)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9876)
    args = ap.parse_args()

    app = App(args.host, args.port)
    app.mainloop()


if __name__ == "__main__":
    main()
