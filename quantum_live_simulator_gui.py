import random
import threading
import time
import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

try:
    import serial
except Exception:
    serial = None

from quantum_live_simulator import (
    DIST_TO_CHECKPOINTS,
    build_heat_script,
    choose_bibs,
    fallback_bibs,
    load_bibs_from_xlsx,
)


class SerialSink:
    def __init__(self):
        self.ser = None

    def is_open(self) -> bool:
        return self.ser is not None

    def open(self, port: str, baud: int):
        if serial is None:
            raise RuntimeError("pyserial not installed")
        self.close()
        self.ser = serial.Serial(
            port,
            baud,
            timeout=0.2,
            bytesize=serial.EIGHTBITS,
            parity=serial.PARITY_NONE,
            stopbits=serial.STOPBITS_ONE,
            xonxoff=False,
            rtscts=False,
            dsrdtr=False,
        )

    def close(self):
        if self.ser is not None:
            try:
                self.ser.close()
            except Exception:
                pass
            self.ser = None

    def send(self, line: str):
        if self.ser is None:
            return
        msg = (line.rstrip() + "\n").encode("ascii", errors="ignore")
        self.ser.write(msg)
        self.ser.flush()


class SimGui(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Quantum Live Simulator - Advanced")
        self.geometry("1120x760")
        self.minsize(980, 680)

        self.sink = SerialSink()
        self.auto_thr = None
        self.auto_stop = threading.Event()

        self.var_port = tk.StringVar(value="COM7")
        self.var_baud = tk.StringVar(value="9600")
        self.var_xlsx = tk.StringVar(value="тест2.xlsx")
        self.var_race = tk.StringVar(value="1")
        self.var_heat = tk.StringVar(value="1")
        self.var_distance = tk.StringVar(value="500")
        self.var_riders = tk.StringVar(value="2")
        self.var_bib_a = tk.StringVar(value="")
        self.var_bib_b = tk.StringVar(value="")
        self.var_heats = tk.StringVar(value="10")
        self.var_speed = tk.StringVar(value="1.0")
        self.var_jitter = tk.StringVar(value="0.04")
        self.var_gap = tk.StringVar(value="1.0")
        self.var_loop = tk.BooleanVar(value=False)
        self.var_noise = tk.IntVar(value=0)
        self.var_noise_enable = tk.BooleanVar(value=False)
        self.var_raw = tk.StringVar(value="")
        self.var_delay = tk.StringVar(value="0")
        self.var_delay_enable = tk.BooleanVar(value=False)
        self.var_contacts_only = tk.BooleanVar(value=True)
        self.var_overlap = tk.BooleanVar(value=False)
        self.var_unconfirmed = tk.BooleanVar(value=False)
        self.var_active_lane = tk.StringVar(value="B")

        self.status_var = tk.StringVar(value="Disconnected")
        self.timer_a_var = tk.StringVar(value="0.000")
        self.timer_b_var = tk.StringVar(value="0.000")
        self.pulse_a_var = tk.StringVar(value="0")
        self.pulse_b_var = tk.StringVar(value="0")

        self.started_at = None
        self.finish_a: float = 0.0
        self.finish_b: float = 0.0
        self.done_a = False
        self.done_b = False
        self.last_line_a = ""
        self.last_line_b = ""
        self.armed_finish = False
        self.bib_pool = fallback_bibs()
        self.bib_cursor = 0

        self._build_ui()
        self._bind_hotkeys()
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.after(80, self._tick_timers)

    def _build_ui(self):
        root = ttk.Frame(self, padding=10)
        root.pack(fill="both", expand=True)

        top = ttk.LabelFrame(root, text="Connection", padding=8)
        top.pack(fill="x")
        ttk.Label(top, text="COM").pack(side="left")
        ttk.Entry(top, textvariable=self.var_port, width=10).pack(side="left", padx=(6, 10))
        ttk.Label(top, text="Baud").pack(side="left")
        ttk.Combobox(top, textvariable=self.var_baud, values=["9600", "19200", "38400", "57600", "115200"], state="readonly", width=10).pack(side="left", padx=(6, 10))
        ttk.Button(top, text="Open", command=self.open_port).pack(side="left")
        ttk.Button(top, text="Close", command=self.close_port).pack(side="left", padx=(6, 10))
        ttk.Label(top, textvariable=self.status_var).pack(side="right")

        nb = ttk.Notebook(root)
        nb.pack(fill="both", expand=True, pady=(8, 0))

        tab_timing = ttk.Frame(nb)
        tab_pass = ttk.Frame(nb)
        tab_noise = ttk.Frame(nb)
        nb.add(tab_timing, text="Timing")
        nb.add(tab_pass, text="Passing List")
        nb.add(tab_noise, text="Noise Level")

        self._build_timing_tab(tab_timing)
        self._build_pass_tab(tab_pass)
        self._build_noise_tab(tab_noise)

    def _build_timing_tab(self, parent):
        root = ttk.Frame(parent, padding=8)
        root.pack(fill="both", expand=True)

        top = ttk.Frame(root)
        top.pack(fill="x")
        ttk.Label(top, text="Heat:", font=("Segoe UI", 16, "bold")).pack(side="left", padx=(0, 6))
        ttk.Entry(top, textvariable=self.var_heat, width=6, font=("Segoe UI", 18, "bold")).pack(side="left")
        ttk.Button(top, text="Heat", command=self.send_dn).pack(side="left", padx=(8, 6))
        ttk.Button(top, text="Bibs", command=self.send_da).pack(side="left", padx=(0, 6))
        ttk.Button(top, text="CD Stop", command=self.cd_stop).pack(side="left", padx=(0, 14))

        ttk.Label(top, text="Race").pack(side="left")
        ttk.Entry(top, textvariable=self.var_race, width=6).pack(side="left", padx=(4, 10))
        ttk.Label(top, text="Distance").pack(side="left")
        ttk.Combobox(top, textvariable=self.var_distance, values=["125", "250", "500", "1000", "2000"], state="readonly", width=7).pack(side="left", padx=(4, 10))
        ttk.Label(top, text="Active").pack(side="left")
        ttk.Combobox(top, textvariable=self.var_active_lane, values=["A", "B"], state="readonly", width=4).pack(side="left", padx=(4, 10))

        main = ttk.Frame(root)
        main.pack(fill="both", expand=True, pady=(8, 0))
        left = ttk.Frame(main)
        right = ttk.Frame(main)
        left.pack(side="left", fill="both", expand=True)
        right.pack(side="left", fill="both", expand=True, padx=(10, 0))

        riders = ttk.Frame(left)
        riders.pack(fill="both", expand=True)
        ra = ttk.LabelFrame(riders, text="Rider A", padding=8)
        rb = ttk.LabelFrame(riders, text="Rider B", padding=8)
        ra.pack(side="left", fill="both", expand=True, padx=(0, 6))
        rb.pack(side="left", fill="both", expand=True, padx=(6, 0))

        self._build_rider_panel(ra, lane="A")
        self._build_rider_panel(rb, lane="B")

        ctrl = ttk.LabelFrame(left, text="Control", padding=8)
        ctrl.pack(fill="x", pady=(10, 0))
        row1 = ttk.Frame(ctrl)
        row1.pack(fill="x")
        ttk.Button(row1, text="False Start (F3)", command=self.false_start).pack(side="left", padx=(0, 6))
        ttk.Button(row1, text="Start (F2)", command=self.send_ds).pack(side="left", padx=(0, 6))
        ttk.Button(row1, text="Pursuit A (F9)", command=lambda: self.set_active_lane("A")).pack(side="left", padx=(0, 6))
        ttk.Button(row1, text="Pursuit B (F10)", command=lambda: self.set_active_lane("B")).pack(side="left", padx=(0, 6))

        row2 = ttk.Frame(ctrl)
        row2.pack(fill="x", pady=(6, 0))
        ttk.Checkbutton(row2, text="Contacts Only (F6)", variable=self.var_contacts_only).pack(side="left", padx=(0, 10))
        ttk.Checkbutton(row2, text="Overlap (F7)", variable=self.var_overlap).pack(side="left", padx=(0, 10))
        ttk.Button(row2, text="R.Close (F8)", command=self.r_close).pack(side="left", padx=(0, 10))
        ttk.Button(row2, text="Arm Finish (Ctrl+F12)", command=self.arm_finish).pack(side="left", padx=(0, 10))

        row3 = ttk.Frame(ctrl)
        row3.pack(fill="x", pady=(8, 0))
        ttk.Label(row3, text="Pulses:").pack(side="left")
        ttk.Combobox(row3, textvariable=self.var_distance, values=["125", "250", "500", "1000", "2000"], state="readonly", width=7).pack(side="left", padx=(4, 10))
        ttk.Label(row3, text="Delay ms:").pack(side="left")
        ttk.Entry(row3, textvariable=self.var_delay, width=6).pack(side="left", padx=(4, 4))
        ttk.Checkbutton(row3, text="Enable", variable=self.var_delay_enable).pack(side="left", padx=(0, 12))
        ttk.Checkbutton(row3, text="Unconfirmed", variable=self.var_unconfirmed).pack(side="left")

        auto = ttk.LabelFrame(left, text="Auto Mode", padding=8)
        auto.pack(fill="x", pady=(10, 0))
        ttk.Label(auto, text="Heats").grid(row=0, column=0, sticky="w")
        ttk.Entry(auto, textvariable=self.var_heats, width=7).grid(row=0, column=1, sticky="w", padx=(4, 10))
        ttk.Label(auto, text="Riders").grid(row=0, column=2, sticky="w")
        ttk.Combobox(auto, textvariable=self.var_riders, values=["1", "2"], state="readonly", width=6).grid(row=0, column=3, sticky="w", padx=(4, 10))
        ttk.Label(auto, text="Speed").grid(row=1, column=0, sticky="w", pady=(6, 0))
        ttk.Entry(auto, textvariable=self.var_speed, width=7).grid(row=1, column=1, sticky="w", padx=(4, 10), pady=(6, 0))
        ttk.Label(auto, text="Jitter").grid(row=1, column=2, sticky="w", pady=(6, 0))
        ttk.Entry(auto, textvariable=self.var_jitter, width=7).grid(row=1, column=3, sticky="w", padx=(4, 10), pady=(6, 0))
        ttk.Label(auto, text="Gap").grid(row=1, column=4, sticky="w", pady=(6, 0))
        ttk.Entry(auto, textvariable=self.var_gap, width=7).grid(row=1, column=5, sticky="w", padx=(4, 10), pady=(6, 0))
        ttk.Checkbutton(auto, text="Loop", variable=self.var_loop).grid(row=0, column=4, columnspan=2, sticky="w")
        ttk.Button(auto, text="Start Auto", command=self.start_auto).grid(row=2, column=0, columnspan=2, sticky="w", pady=(8, 0))
        ttk.Button(auto, text="Stop Auto", command=self.stop_auto).grid(row=2, column=2, columnspan=2, sticky="w", pady=(8, 0))

        src = ttk.LabelFrame(right, text="Bib Source + Raw", padding=8)
        src.pack(fill="x")
        xrow = ttk.Frame(src)
        xrow.pack(fill="x")
        ttk.Entry(xrow, textvariable=self.var_xlsx).pack(side="left", fill="x", expand=True)
        ttk.Button(xrow, text="...", width=4, command=self.pick_xlsx).pack(side="left", padx=(6, 0))
        ttk.Button(src, text="Load Bibs", command=self.load_bibs).pack(anchor="w", pady=(6, 0))
        ttk.Button(src, text="Next Bibs", command=self.next_bibs).pack(anchor="w", pady=(4, 0))

        raw = ttk.Frame(src)
        raw.pack(fill="x", pady=(10, 0))
        ttk.Entry(raw, textvariable=self.var_raw).pack(side="left", fill="x", expand=True)
        ttk.Button(raw, text="Send Raw", command=self.send_raw).pack(side="left", padx=(6, 0))

        log_box = ttk.LabelFrame(right, text="Event Stream", padding=8)
        log_box.pack(fill="both", expand=True, pady=(8, 0))
        self.log = tk.Text(log_box, wrap="none", font=("Consolas", 11))
        y = ttk.Scrollbar(log_box, orient="vertical", command=self.log.yview)
        self.log.configure(yscrollcommand=y.set)
        self.log.pack(side="left", fill="both", expand=True)
        y.pack(side="right", fill="y")

    def _build_rider_panel(self, parent, lane: str):
        bib_var = self.var_bib_a if lane == "A" else self.var_bib_b
        timer_var = self.timer_a_var if lane == "A" else self.timer_b_var
        pulse_var = self.pulse_a_var if lane == "A" else self.pulse_b_var

        ttk.Label(parent, text="Bib:").pack(anchor="w")
        ttk.Entry(parent, textvariable=bib_var, width=12, font=("Segoe UI", 16, "bold")).pack(anchor="w", pady=(2, 8))
        ttk.Label(parent, textvariable=timer_var, font=("Segoe UI", 34, "bold")).pack(anchor="center", pady=(4, 10))
        ttk.Label(parent, text="Pulse #:").pack(anchor="w")
        ttk.Entry(parent, textvariable=pulse_var, width=8, font=("Segoe UI", 20, "bold")).pack(anchor="w", pady=(2, 8))

        btns = ttk.Frame(parent)
        btns.pack(fill="x", pady=(4, 0))
        ttk.Button(btns, text=("F4 +" if lane == "A" else "F5 +"), command=lambda l=lane: self.manual_split(l)).pack(side="left", padx=(0, 6))
        ttk.Button(btns, text="Finish", command=lambda l=lane: self.manual_finish(l)).pack(side="left", padx=(0, 6))
        ttk.Button(btns, text="Resend", command=lambda l=lane: self.resend_lane(l)).pack(side="left", padx=(0, 6))
        ttk.Button(btns, text="Status", command=lambda l=lane: self.status_lane(l)).pack(side="left")

    def _build_pass_tab(self, parent):
        wrap = ttk.Frame(parent, padding=8)
        wrap.pack(fill="both", expand=True)
        self.pass_tv = ttk.Treeview(wrap, columns=("ts", "line"), show="headings")
        self.pass_tv.heading("ts", text="Timestamp")
        self.pass_tv.heading("line", text="Message")
        self.pass_tv.column("ts", width=130, anchor="center")
        self.pass_tv.column("line", width=840, anchor="w")
        y = ttk.Scrollbar(wrap, orient="vertical", command=self.pass_tv.yview)
        self.pass_tv.configure(yscrollcommand=y.set)
        self.pass_tv.pack(side="left", fill="both", expand=True)
        y.pack(side="right", fill="y")

    def _build_noise_tab(self, parent):
        wrap = ttk.Frame(parent, padding=12)
        wrap.pack(fill="both", expand=True)
        ttk.Checkbutton(wrap, text="Enable noise injection in auto mode", variable=self.var_noise_enable).pack(anchor="w")
        ttk.Label(wrap, text="Noise level (%)").pack(anchor="w", pady=(8, 0))
        ttk.Scale(wrap, from_=0, to=100, variable=self.var_noise, orient="horizontal").pack(fill="x")
        ttk.Label(
            wrap,
            text="When enabled, random junk lines are inserted during auto run to emulate line noise.",
            foreground="#666",
        ).pack(anchor="w", pady=(8, 0))

    def _append_log(self, line: str):
        ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        self.log.insert("end", f"{ts}  {line.rstrip()}\n")
        self.log.see("end")

    def _add_pass(self, line: str):
        ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        iid = self.pass_tv.insert("", "end", values=(ts, line.rstrip()))
        self.pass_tv.see(iid)

    def _emit_now(self, line: str):
        line = line.strip()
        if not line:
            return
        if self.sink.is_open():
            try:
                self.sink.send(line)
            except Exception as e:
                self._append_log(f"ERR write: {e}")
        self._append_log(line)
        self._add_pass(line)
        self._apply_line_to_panel(line)

    def _emit(self, line: str):
        line = str(line or "").strip()
        if not line:
            return
        if self.var_delay_enable.get():
            try:
                delay_ms = max(0, int(float(self.var_delay.get().strip() or "0")))
            except Exception:
                delay_ms = 0
            self.after(delay_ms, lambda l=line: self._emit_now(l))
        else:
            self._emit_now(line)

    def _current_cp(self) -> int:
        try:
            d = int(self.var_distance.get().strip() or "500")
        except Exception:
            d = 500
        return int(DIST_TO_CHECKPOINTS.get(d, 4))

    def _lane_by_bib(self, bib: str) -> str:
        b = str(bib or "").strip()
        if b and b == self.var_bib_a.get().strip():
            return "A"
        if b and b == self.var_bib_b.get().strip():
            return "B"
        return ""

    def _apply_line_to_panel(self, line: str):
        raw = str(line or "").strip()
        if not raw:
            return
        parts = [p.strip() for p in raw.split("|")]
        if not parts:
            return
        cmd = parts[0]

        if cmd == "DN":
            if len(parts) > 1:
                self.var_race.set(parts[1])
            if len(parts) > 2:
                self.var_heat.set(parts[2])
            return

        if cmd == "DA":
            bibs = [p for p in parts[3:] if p]
            if len(bibs) > 0:
                self.var_bib_a.set(bibs[0])
            if len(bibs) > 1:
                self.var_bib_b.set(bibs[1])
            return

        if cmd == "DS":
            self.started_at = time.monotonic()
            self.finish_a = 0.0
            self.finish_b = 0.0
            self.done_a = False
            self.done_b = False
            self.pulse_a_var.set("0")
            self.pulse_b_var.set("0")
            bibs = [p for p in parts[3:] if p and ":" not in p]
            if len(bibs) > 0:
                self.var_bib_a.set(bibs[0])
            if len(bibs) > 1:
                self.var_bib_b.set(bibs[1])
            return

        if cmd not in ("DI", "DF"):
            return

        if len(parts) < 5:
            return
        split_no = parts[3]
        bib = parts[4]
        lane = self._lane_by_bib(bib)
        try:
            t = None
            for p in parts[5:]:
                if not p:
                    continue
                t = float(p)
                break
        except Exception:
            t = None

        if lane == "A":
            if split_no.isdigit():
                self.pulse_a_var.set(str(int(split_no)))
            self.last_line_a = raw
            if cmd == "DF" and t is not None:
                self.finish_a = float(t)
                self.done_a = True
        elif lane == "B":
            if split_no.isdigit():
                self.pulse_b_var.set(str(int(split_no)))
            self.last_line_b = raw
            if cmd == "DF" and t is not None:
                self.finish_b = float(t)
                self.done_b = True

    def open_port(self):
        try:
            self.sink.open(self.var_port.get().strip(), int(self.var_baud.get().strip()))
            self.status_var.set(f"Connected: {self.var_port.get().strip()}")
        except Exception as e:
            messagebox.showerror("Open error", str(e))

    def close_port(self):
        self.sink.close()
        self.status_var.set("Disconnected")

    def pick_xlsx(self):
        p = filedialog.askopenfilename(title="Pick xlsx", filetypes=[("Excel", "*.xlsx"), ("All", "*.*")])
        if p:
            self.var_xlsx.set(p)

    def load_bibs(self):
        try:
            p = Path(self.var_xlsx.get().strip())
            self.bib_pool = load_bibs_from_xlsx(p)
            self.bib_cursor = 0
            self._append_log(f"Loaded bibs: {len(self.bib_pool)}")
        except Exception as e:
            self.bib_pool = fallback_bibs()
            self.bib_cursor = 0
            self._append_log(f"WARN load xlsx: {e}; using fallback")

    def next_bibs(self):
        if not self.bib_pool:
            self.load_bibs()
        riders = max(1, min(2, int(self.var_riders.get().strip() or "2")))
        heat_i = int(self.var_heat.get().strip() or "1") + self.bib_cursor
        bs = choose_bibs(self.bib_pool, heat_i, riders)
        self.bib_cursor += 1
        self.var_bib_a.set(bs[0] if len(bs) > 0 else "")
        self.var_bib_b.set(bs[1] if len(bs) > 1 else "")

    def _race_heat(self):
        return int(self.var_race.get().strip() or "1"), int(self.var_heat.get().strip() or "1")

    def _bind_hotkeys(self):
        self.bind("<F2>", lambda _e: self.send_ds())
        self.bind("<F3>", lambda _e: self.false_start())
        self.bind("<F4>", lambda _e: self.manual_split("A"))
        self.bind("<F5>", lambda _e: self.manual_split("B"))
        self.bind("<F6>", lambda _e: self.var_contacts_only.set(not self.var_contacts_only.get()))
        self.bind("<F7>", lambda _e: self.var_overlap.set(not self.var_overlap.get()))
        self.bind("<F8>", lambda _e: self.r_close())
        self.bind("<F9>", lambda _e: self.set_active_lane("A"))
        self.bind("<F10>", lambda _e: self.set_active_lane("B"))
        self.bind("<Control-F12>", lambda _e: self.arm_finish())

    def set_active_lane(self, lane: str):
        self.var_active_lane.set("A" if str(lane).upper() == "A" else "B")
        self._append_log(f"MODE: Pursuit {self.var_active_lane.get()}")

    def cd_stop(self):
        self.stop_auto()
        self._append_log("CD STOP")

    def r_close(self):
        self.close_port()
        self._append_log("R.CLOSE")

    def arm_finish(self):
        self.armed_finish = not self.armed_finish
        self._append_log(f"ARM FINISH: {'ON' if self.armed_finish else 'OFF'}")

    def resend_lane(self, lane: str):
        line = self.last_line_a if lane == "A" else self.last_line_b
        if not line:
            self._append_log(f"RESEND {lane}: empty")
            return
        self._append_log(f"RESEND {lane}")
        self._emit(line)

    def status_lane(self, lane: str):
        bib = self.var_bib_a.get().strip() if lane == "A" else self.var_bib_b.get().strip()
        pulse = self.pulse_a_var.get().strip() if lane == "A" else self.pulse_b_var.get().strip()
        t = self.timer_a_var.get().strip() if lane == "A" else self.timer_b_var.get().strip()
        msg = f"STATUS {lane}: bib={bib or '-'} pulse={pulse or '0'} time={t or '0.000'}"
        self._append_log(msg)

    def _bibs(self):
        a = self.var_bib_a.get().strip()
        b = self.var_bib_b.get().strip()
        out = []
        if a:
            out.append(a)
        if b and (self.var_overlap.get() or b != a):
            out.append(b)
        return out

    def send_dn(self):
        race, heat = self._race_heat()
        self._emit(f"DN| {race}| {heat}|")

    def send_da(self):
        race, heat = self._race_heat()
        bibs = self._bibs()
        self._emit(f"DA|  {race}| {heat}|" + "|".join(bibs))

    def send_ds(self):
        race, heat = self._race_heat()
        bibs = self._bibs()
        st = datetime.now().strftime("%H:%M:%S.000")
        self._emit(f"DS|  {race}| {heat}|" + "|".join(bibs) + f"|{st}")
        self.started_at = time.monotonic()
        self.finish_a = 0.0
        self.finish_b = 0.0
        self.done_a = False
        self.done_b = False
        self.pulse_a_var.set("0")
        self.pulse_b_var.set("0")

    def false_start(self):
        race, heat = self._race_heat()
        self._emit(f"FS| {race}| {heat}| false start")

    def _elapsed(self) -> float:
        if self.started_at is None:
            return 0.0
        return max(0.0, time.monotonic() - self.started_at)

    def manual_split(self, lane: str):
        if self.started_at is None:
            messagebox.showwarning("Warning", "Start heat first")
            return
        lane = "A" if str(lane).upper() == "A" else "B"
        if self.var_contacts_only.get() and lane != self.var_active_lane.get():
            self._append_log(f"IGNORED {lane}: contacts-only active lane is {self.var_active_lane.get()}")
            return
        if lane == "A" and self.done_a:
            return
        if lane == "B" and self.done_b:
            return
        race, heat = self._race_heat()
        bib = self.var_bib_a.get().strip() if lane == "A" else self.var_bib_b.get().strip()
        if not bib:
            return
        cp = self._current_cp()
        if lane == "A":
            pulse = int(self.pulse_a_var.get() or "0") + 1
            self.pulse_a_var.set(str(pulse))
        else:
            pulse = int(self.pulse_b_var.get() or "0") + 1
            self.pulse_b_var.set(str(pulse))
        t = self._elapsed()
        if self.armed_finish:
            self.armed_finish = False
            self._emit(f"DF| {race}| {heat}| {cp}|{bib}|   |      {t:.3f}|")
            if lane == "A":
                self.done_a = True
                self.finish_a = t
                self.pulse_a_var.set(str(cp))
            else:
                self.done_b = True
                self.finish_b = t
                self.pulse_b_var.set(str(cp))
            return
        if pulse >= cp:
            self._emit(f"DF| {race}| {heat}| {cp}|{bib}|   |      {t:.3f}|")
            if lane == "A":
                self.done_a = True
                self.finish_a = t
                self.pulse_a_var.set(str(cp))
            else:
                self.done_b = True
                self.finish_b = t
                self.pulse_b_var.set(str(cp))
        else:
            self._emit(f"DI| {race}| {heat}| {pulse}|{bib}|      {t:.3f}|")

    def manual_finish(self, lane: str):
        if self.started_at is None:
            messagebox.showwarning("Warning", "Start heat first")
            return
        lane = "A" if str(lane).upper() == "A" else "B"
        if self.var_contacts_only.get() and lane != self.var_active_lane.get():
            self._append_log(f"IGNORED {lane}: contacts-only active lane is {self.var_active_lane.get()}")
            return
        if lane == "A" and self.done_a:
            return
        if lane == "B" and self.done_b:
            return
        race, heat = self._race_heat()
        bib = self.var_bib_a.get().strip() if lane == "A" else self.var_bib_b.get().strip()
        if not bib:
            return
        cp = self._current_cp()
        t = self._elapsed()
        self._emit(f"DF| {race}| {heat}| {cp}|{bib}|   |      {t:.3f}|")
        if lane == "A":
            self.done_a = True
            self.finish_a = t
            self.pulse_a_var.set(str(cp))
        else:
            self.done_b = True
            self.finish_b = t
            self.pulse_b_var.set(str(cp))

    def send_raw(self):
        self._emit(self.var_raw.get().strip())

    def _tick_timers(self):
        t = self._elapsed()
        self.timer_a_var.set(f"{self.finish_a:.3f}" if self.done_a else f"{t:.3f}")
        self.timer_b_var.set(f"{self.finish_b:.3f}" if self.done_b else f"{t:.3f}")
        self.after(80, self._tick_timers)

    def _maybe_noise(self):
        if not self.var_noise_enable.get():
            return
        level = max(0, min(100, int(self.var_noise.get())))
        if random.randint(1, 100) <= level:
            garbage = random.choice([
                "@@@NOISE###",
                "XXXXXX",
                "\x00\x00\x00",
                "RANDOM|BROKEN|LINE",
            ])
            self._emit(garbage)

    def start_auto(self):
        if self.auto_thr and self.auto_thr.is_alive():
            return
        self.auto_stop.clear()
        self.auto_thr = threading.Thread(target=self._auto_loop, daemon=True)
        self.auto_thr.start()
        self._append_log("AUTO START")

    def stop_auto(self):
        self.auto_stop.set()
        self._append_log("AUTO STOP")

    def _auto_loop(self):
        try:
            race = int(self.var_race.get().strip() or "1")
            heat0 = int(self.var_heat.get().strip() or "1")
            heats = int(self.var_heats.get().strip() or "10")
            riders = max(1, min(2, int(self.var_riders.get().strip() or "2")))
            dist = int(self.var_distance.get().strip() or "500")
            cp = DIST_TO_CHECKPOINTS.get(dist, 4)
            speed = max(0.05, float(self.var_speed.get().strip() or "1.0"))
            jitter = max(0.0, float(self.var_jitter.get().strip() or "0.04"))
            gap = max(0.0, float(self.var_gap.get().strip() or "1.0"))

            if not self.bib_pool:
                self.bib_pool = fallback_bibs()

            loop = 0
            while not self.auto_stop.is_set():
                loop += 1
                for i in range(heats):
                    if self.auto_stop.is_set():
                        break
                    heat = heat0 + i
                    bibs = choose_bibs(self.bib_pool, i + 1 + (loop - 1) * heats, riders)
                    script = build_heat_script(race, heat, bibs, cp, dist, jitter)
                    t0 = time.monotonic()
                    for ev in script:
                        if self.auto_stop.is_set():
                            break
                        if self.var_contacts_only.get() and ev.line.startswith(("DI|", "DF|")):
                            parts = [p.strip() for p in ev.line.split("|")]
                            bib = parts[4] if len(parts) > 4 else ""
                            if self.var_active_lane.get() == "A":
                                lane_bib = bibs[0] if len(bibs) > 0 else ""
                            else:
                                lane_bib = bibs[1] if len(bibs) > 1 else (bibs[0] if len(bibs) == 1 else "")
                            if bib and lane_bib and bib != lane_bib:
                                continue
                        due = t0 + ev.offset_sec / speed
                        wait = due - time.monotonic()
                        if wait > 0:
                            time.sleep(wait)
                        self.after(0, self._emit, ev.line)
                        self.after(0, self._maybe_noise)
                    if self.auto_stop.is_set():
                        break
                    if gap > 0:
                        time.sleep(gap / speed)
                if not self.var_loop.get():
                    break
                race += 1
        except Exception as e:
            self.after(0, self._append_log, f"AUTO ERR: {e}")

    def _on_close(self):
        self.stop_auto()
        self.close_port()
        self.destroy()


def main():
    app = SimGui()
    app.mainloop()


if __name__ == "__main__":
    main()
