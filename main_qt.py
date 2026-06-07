import argparse
import csv
import datetime
import hashlib
import json
import os
import queue
import re
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from core_server import (
    DIST_STEP_M,
    MeetModel,
    OverlayHttp,
    ReaderThread,
    TcpJsonlServer,
    TcpStateServer,
    _atomic_write_text,
    cc_short,
    flag_for_excel_value,
    fmt_live,
    fmt_time,
    last_split_num,
    load_workbook,
    resource_path,
    split_sort_key,
)
from stats_sender import AsyncStatsSender

try:
    from serial.tools import list_ports
except Exception:
    list_ports = None

try:
    from PySide6.QtCore import QTimer, Qt
    from PySide6.QtGui import QColor, QFont
    from PySide6.QtWidgets import (
        QApplication,
        QCheckBox,
        QComboBox,
        QFileDialog,
        QFrame,
        QHBoxLayout,
        QHeaderView,
        QLabel,
        QLineEdit,
        QMainWindow,
        QMessageBox,
        QPushButton,
        QPlainTextEdit,
        QSplitter,
        QStatusBar,
        QTabWidget,
        QTableWidget,
        QTableWidgetItem,
        QVBoxLayout,
        QWidget,
    )
except Exception:
    import traceback
    err = traceback.format_exc()
    if getattr(sys, "frozen", False):
        print("Qt runtime initialization failed in bundled executable.")
        print(err)
    else:
        print("PySide6 is required. Install with: py -3 -m pip install PySide6")
        print(err)
    sys.exit(1)


class QtServerApp(QMainWindow):
    def __init__(self, listen_host: str, listen_port: int, stats_endpoint: str = ""):
        super().__init__()
        self.setWindowTitle("SwissTiming Quantum Viewer (Qt)")
        self.resize(1620, 980)

        self.q = queue.Queue()
        self.stop_evt = threading.Event()
        self.reader: Optional[ReaderThread] = None

        self.model = MeetModel()
        self.selected_run_key: Optional[str] = None
        self.excel_names_by_category: Dict[str, Dict[str, str]] = {}
        self.excel_country_by_category: Dict[str, Dict[str, str]] = {}
        self.current_category: str = ""
        self._last_split_count_by_run: Dict[str, int] = {}
        self._evt_processed = 0
        self._evt_skipped = 0
        self._render_errors = 0
        self._last_diag_total = -1
        self._ui_dirty = False
        self._last_ui_refresh_ts = 0.0
        self._last_state_broadcast_ts = 0.0
        self.stats_sender = AsyncStatsSender(endpoint=stats_endpoint)
        self.stats_sender.start()

        self._obs_lock = threading.Lock()
        self._obs_payload = {"ts": 0, "run": "", "left": {}, "right": {}}
        self.obs_json = str(Path.cwd() / "obs_state.json")

        overlay_path = resource_path("overlay_test.html")
        flags_dir = resource_path("flags")
        self.overlay_http = OverlayHttp(self, host="0.0.0.0", port=8099, overlay_html_path=overlay_path, flags_dir=flags_dir)
        self.overlay_http.start()

        self.net = TcpStateServer(listen_host, listen_port, on_error=self._net_error)
        self.net.start()
        self.net.set_last_state(self._model_to_state())

        self.live_tcp = TcpJsonlServer(listen_host, 8098, on_error=self._net_error)
        self.live_tcp.start()
        self.live_tcp.set_last({"ts": 0, "run": "", "left": {}, "right": {}})

        self._build_ui()
        self._apply_qss()

        self.timer = QTimer(self)
        self.timer.timeout.connect(self._pump)
        self.timer.start(50)

        if self.stats_sender.enabled:
            self._append_log(f"STATS: enabled -> {self.stats_sender.endpoint}")
        else:
            self._append_log("STATS: disabled (set --stats-endpoint or SWISS_STATS_ENDPOINT)")

    def _build_ui(self):
        root = QWidget(self)
        self.setCentralWidget(root)
        main = QVBoxLayout(root)
        main.setContentsMargins(20, 18, 20, 18)
        main.setSpacing(12)

        title = QLabel("SwissTiming Quantum Viewer")
        title.setObjectName("Title")
        sub = QLabel("Qt edition · race control · live timing · overlay")
        sub.setObjectName("Sub")

        bar = QFrame()
        bar.setObjectName("Card")
        bar_l = QHBoxLayout(bar)
        bar_l.setContentsMargins(14, 14, 14, 14)
        bar_l.setSpacing(10)

        self.port_cb = QComboBox()
        self.port_cb.setMinimumWidth(170)
        self._refresh_ports()
        self.baud_cb = QComboBox()
        self.baud_cb.addItems(["9600", "19200", "38400", "57600", "115200"])
        self.baud_cb.setCurrentText("9600")
        self.baud_cb.setMinimumWidth(130)

        bar_l.addWidget(QLabel("COM"))
        bar_l.addWidget(self.port_cb)
        bar_l.addWidget(QLabel("Baud"))
        bar_l.addWidget(self.baud_cb)

        btn_refresh = QPushButton("Обновить")
        btn_refresh.clicked.connect(self._refresh_ports)
        btn_connect = QPushButton("Подключить")
        btn_connect.setObjectName("Accent")
        btn_connect.clicked.connect(self.connect_serial)
        btn_disconnect = QPushButton("Отключить")
        btn_disconnect.clicked.connect(self.disconnect)
        btn_file = QPushButton("Файл")
        btn_file.clicked.connect(self.replay_file)
        btn_excel = QPushButton("Excel")
        btn_excel.clicked.connect(self.load_excel)
        btn_csv = QPushButton("CSV")
        btn_csv.clicked.connect(self.export_csv)

        self.show_dist_cb = QCheckBox("Дистанция")
        self.show_dist_cb.setChecked(True)
        self.show_dist_cb.toggled.connect(self._refresh_athletes)

        for w in [btn_refresh, btn_connect, btn_disconnect, btn_file, btn_excel, btn_csv, self.show_dist_cb]:
            if hasattr(w, "setMinimumHeight"):
                w.setMinimumHeight(40)
            bar_l.addWidget(w)

        bar_l.addStretch(1)
        self.status_lbl = QLabel("Отключено")
        self.status_lbl.setObjectName("StatusBad")
        self.status_lbl.setMinimumHeight(38)
        bar_l.addWidget(self.status_lbl)
        main.addWidget(bar)

        tabs = QTabWidget()
        main.addWidget(tabs, 1)

        tab_res = QWidget()
        tab_log = QWidget()
        tabs.addTab(tab_res, "Результаты")
        tabs.addTab(tab_log, "Сырые данные")

        res_l = QVBoxLayout(tab_res)
        splitter = QSplitter(Qt.Horizontal)
        res_l.addWidget(splitter)

        left = QFrame(); left.setObjectName("Card")
        right = QFrame(); right.setObjectName("Card")
        splitter.addWidget(left)
        splitter.addWidget(right)
        splitter.setSizes([440, 1160])

        ll = QVBoxLayout(left)
        ll.setContentsMargins(14, 14, 14, 14)
        ll.setSpacing(10)
        ll.addWidget(QLabel("Заезды"))

        cat_row = QHBoxLayout()
        cat_row.addWidget(QLabel("Категория"))
        self.category_cb = QComboBox()
        self.category_cb.currentTextChanged.connect(self._on_category_change)
        cat_row.addWidget(self.category_cb, 1)
        ll.addLayout(cat_row)

        self.run_filter = QLineEdit()
        self.run_filter.setPlaceholderText("Фильтр заездов")
        self.run_filter.textChanged.connect(self._refresh_runs)
        self.run_filter.setMinimumHeight(40)
        ll.addWidget(self.run_filter)

        self.runs_table = QTableWidget(0, 5)
        self.runs_table.setHorizontalHeaderLabels(["Заезд", "Кат.", "Старт", "Участн.", "Финиш"])
        self.runs_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.runs_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.runs_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.runs_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.runs_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)
        self.runs_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.runs_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.runs_table.setAlternatingRowColors(True)
        self.runs_table.verticalHeader().setVisible(False)
        self.runs_table.verticalHeader().setDefaultSectionSize(42)
        self.runs_table.setShowGrid(False)
        self.runs_table.setFocusPolicy(Qt.StrongFocus)
        self.runs_table.cellClicked.connect(self._on_run_selected)
        ll.addWidget(self.runs_table, 1)

        rl = QVBoxLayout(right)
        rl.setContentsMargins(14, 14, 14, 14)
        rl.setSpacing(10)
        top_row = QHBoxLayout()
        top_row.addWidget(QLabel("Участники"))
        top_row.addStretch(1)
        self.run_info = QLabel("—")
        self.run_info.setObjectName("Sub")
        top_row.addWidget(self.run_info)
        rl.addLayout(top_row)

        live = QFrame(); live.setObjectName("Panel")
        live_l = QVBoxLayout(live)
        live_l.setContentsMargins(14, 14, 14, 14)
        live_l.setSpacing(8)
        self.live_run = QLabel("Заезд: —")
        self.live_run.setObjectName("LiveRun")
        self.live_left = QLabel("")
        self.live_left.setObjectName("LiveAth")
        self.live_left.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self.live_right = QLabel("")
        self.live_right.setObjectName("LiveAth")
        self.live_right.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        live_l.addWidget(QLabel("Текущее время (2 гонщика)"))
        live_l.addWidget(self.live_run)
        row = QHBoxLayout()
        row.setSpacing(10)
        row.addWidget(self.live_left, 1)
        row.addWidget(self.live_right, 1)
        live_l.addLayout(row)
        rl.addWidget(live)

        self.ath_table = QTableWidget(0, 4)
        self.ath_table.setHorizontalHeaderLabels(["№", "Имя", "Финиш", "Статус"])
        self.ath_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.ath_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.ath_table.setAlternatingRowColors(True)
        self.ath_table.verticalHeader().setVisible(False)
        self.ath_table.verticalHeader().setDefaultSectionSize(42)
        self.ath_table.setShowGrid(False)
        self.ath_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.ath_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.ath_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.ath_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        rl.addWidget(self.ath_table, 1)



        log_l = QVBoxLayout(tab_log)
        self.log = QPlainTextEdit()
        self.log.setReadOnly(True)
        self.log.document().setMaximumBlockCount(500)
        log_l.addWidget(self.log)

        sb = QStatusBar()
        self.setStatusBar(sb)
        sb.showMessage("Готово")

    def _apply_qss(self):
        self.setStyleSheet(
            """
            QMainWindow, QWidget { background: #0b1118; color: #edf2f8; font-family: 'Segoe UI'; font-size: 14px; }
            #Title { font-size: 30px; font-weight: 800; letter-spacing: 0.5px; }
            #Sub { color: #95a9bf; font-size: 14px; }
            #Card { background: #14202c; border-radius: 16px; border: 1px solid #1f3144; }
            #Panel { background: #101b28; border-radius: 14px; border: 1px solid #22364a; }
            QLabel#StatusBad { background: #432028; color: #ff9a9a; border-radius: 10px; padding: 8px 12px; font-weight: 600; }
            QLabel#StatusOk { background: #1d4031; color: #aaf0c8; border-radius: 10px; padding: 8px 12px; font-weight: 600; }
            QLabel#Legend { background: #302819; color: #f5d287; border-radius: 9px; padding: 6px 10px; }
            QLabel#LiveRun { font-size: 18px; font-weight: 700; color: #9fd7ff; }
            QLabel#LiveAth { background: #0d1723; border: 1px solid #263a4f; border-radius: 10px; padding: 12px; font-size: 24px; font-weight: 700; }
            QPushButton { background: #1e2f42; color: #edf2f8; border: none; border-radius: 10px; padding: 10px 14px; font-weight: 600; }
            QPushButton:hover { background: #27455f; }
            QPushButton:pressed { background: #1d354b; }
            QPushButton#Accent { background: #54c4ff; color: #08131f; font-weight: 800; }
            QPushButton#Accent:hover { background: #7bd2ff; }
            QLineEdit, QPlainTextEdit, QTableWidget { background: #0f1a27; border: 1px solid #2a3f54; border-radius: 10px; }
            QHeaderView::section { background: #1b2a3b; color: #edf2f8; border: none; padding: 9px 8px; font-size: 13px; font-weight: 700; }
            QTableWidget { alternate-background-color: #111e2c; selection-background-color: #2b4560; selection-color: #f4f9ff; }
            QComboBox { background: #0f1a27; border: 1px solid #2a3f54; border-radius: 10px; padding: 8px 12px; min-height: 22px; }
            QComboBox::drop-down { border: none; width: 26px; }
            QComboBox QAbstractItemView { background: #0f1a27; color: #edf2f8; selection-background-color: #2b4560; outline: none; }
            QTabWidget::pane { border: none; }
            QTabBar::tab { background: #14202c; color: #9eb0c4; border-radius: 10px; padding: 10px 16px; margin-right: 8px; }
            QTabBar::tab:selected { background: #1e3245; color: #edf2f8; }
            QScrollBar:vertical { background: #0f1a27; width: 12px; margin: 2px; border-radius: 6px; }
            QScrollBar::handle:vertical { background: #2a4157; border-radius: 6px; min-height: 30px; }
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0px; }
            QScrollBar:horizontal { background: #0f1a27; height: 12px; margin: 2px; border-radius: 6px; }
            QScrollBar::handle:horizontal { background: #2a4157; border-radius: 6px; min-width: 30px; }
            QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal { width: 0px; }
            """
        )

    def _list_ports(self) -> List[str]:
        if list_ports is None:
            return []
        return [p.device for p in list_ports.comports()]

    def _refresh_ports(self):
        cur = self.port_cb.currentText()
        ports = self._list_ports()
        self.port_cb.clear()
        self.port_cb.addItems(ports)
        if cur and cur in ports:
            self.port_cb.setCurrentText(cur)

    def _set_status(self, text: str, ok: bool):
        self.status_lbl.setText(text)
        self.status_lbl.setObjectName("StatusOk" if ok else "StatusBad")
        self.status_lbl.style().unpolish(self.status_lbl)
        self.status_lbl.style().polish(self.status_lbl)

    def _append_log(self, s: str):
        if not s:
            return
        self.log.appendPlainText(str(s))

    def _append_trace(self, where: str, exc: Exception, evt: Optional[Dict[str, Any]] = None):
        msg = f"ERROR in {where}: {exc}"
        self._append_log(msg)
        if evt is not None:
            try:
                self._append_log("EVT: " + json.dumps(evt, ensure_ascii=False))
            except Exception:
                self._append_log("EVT: " + str(evt))
        self._append_log(traceback.format_exc())

    def _sanitize_evt(self, evt: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(evt, dict):
            return None
        t = str(evt.get("type") or "").strip().lower()
        if t not in {"new_run", "setup", "start", "split", "finish", "other"}:
            return None

        out = dict(evt)
        out["type"] = t
        if out.get("race") is None:
            out["race"] = "?"
        if out.get("heat") is None:
            out["heat"] = "?"

        if t in ("split", "finish"):
            bib = str(out.get("bib") or "").strip()
            if not bib:
                return None
            out["bib"] = bib
            if t == "split":
                sp = str(out.get("split") or "").strip()
                if not sp:
                    return None
                out["split"] = sp
            tm = out.get("time")
            if tm is not None:
                try:
                    out["time"] = float(tm)
                except Exception:
                    out["time"] = None

        return out

    def _net_error(self, msg: str):
        self.q.put({"kind": "err", "data": f"NET ERROR: {msg}"})

    def _model_to_state(self) -> Dict[str, Any]:
        runs_out: Dict[str, Any] = {}
        for k, run in self.model.runs.items():
            ath = {}
            for bib, a in run.athletes.items():
                ath[bib] = {
                    "bib": bib,
                    "name": a.name,
                    "country": a.country,
                    "splits": dict(a.splits),
                    "finish": a.finish,
                    "status": a.status,
                }
            runs_out[k] = {
                "key": run.key,
                "race": run.race,
                "heat": run.heat,
                "category": run.category,
                "start_time": run.start_time,
                "active_bibs": list(run.active_bibs),
                "bib_order": list(run.bib_order),
                "athletes": ath,
            }
        return {
            "current_key": self.model.current_key,
            "runs": runs_out,
            "categories": sorted(self.excel_names_by_category.keys()),
            "selected_category": self.current_category,
            "ts": time.time(),
        }

    def _apply_selected_category_meta(self):
        cat = self.current_category
        names = self.excel_names_by_category.get(cat, {}) if cat else {}
        countries = self.excel_country_by_category.get(cat, {}) if cat else {}
        self.model.set_bib_meta(names, countries)
        self.net.broadcast_state(self._model_to_state())

    def _on_category_change(self, text: str):
        self.current_category = (text or "").strip()
        self._apply_selected_category_meta()

    def connect_serial(self):
        if self.reader and self.reader.is_alive():
            return
        port = self.port_cb.currentText().strip()
        if not port:
            QMessageBox.critical(self, "Ошибка", "Выбери COM-порт")
            return
        try:
            baud = int(self.baud_cb.currentText().strip())
        except Exception:
            QMessageBox.critical(self, "Ошибка", "Неверный baud")
            return
        self.stop_evt.clear()
        self.reader = ReaderThread(self.q, self.stop_evt, port=port, baud=baud, replay_path=None)
        self.reader.start()
        self._set_status(f"Подключено {port}@{baud}", True)

    def disconnect(self):
        self.stop_evt.set()
        self._set_status("Отключено", False)

    def replay_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Выбери лог", "", "Text (*.txt);;All (*.*)")
        if not path:
            return
        self.disconnect()
        self.model = MeetModel()
        self._apply_selected_category_meta()
        self.selected_run_key = None
        self._refresh_runs()
        self._refresh_athletes()
        self.stop_evt.clear()
        self.reader = ReaderThread(self.q, self.stop_evt, replay_path=path)
        self.reader.start()
        self._set_status("Режим файла", True)

    def load_excel(self):
        if load_workbook is None:
            QMessageBox.critical(self, "Ошибка", "Нужен openpyxl")
            return
        path, _ = QFileDialog.getOpenFileName(self, "Выбери Excel", "", "Excel (*.xlsx);;All (*.*)")
        if not path:
            return
        try:
            wb = load_workbook(path, data_only=True)
            ws = wb.active
            names_by_category: Dict[str, Dict[str, str]] = {}
            countries_by_category: Dict[str, Dict[str, str]] = {}
            for r in ws.iter_rows(min_row=1, max_col=4, values_only=True):
                bib_val = r[0]
                name_val = r[1] if len(r) > 1 else None
                country_val = r[2] if len(r) > 2 else None
                category_val = r[3] if len(r) > 3 else None
                if bib_val is None:
                    continue
                m = re.search(r"(\d+)", str(bib_val))
                if not m:
                    continue
                bib_s = str(int(m.group(1)))
                if bib_s == "0":
                    continue
                name_s = str(name_val).strip() if name_val is not None else ""
                cc_s = str(country_val).strip().upper() if country_val is not None else ""
                cat_s = str(category_val).strip() if category_val is not None else ""
                if not cat_s:
                    cat_s = "Без категории"
                names_by_category.setdefault(cat_s, {})[bib_s] = name_s
                if cc_s:
                    countries_by_category.setdefault(cat_s, {})[bib_s] = cc_s

            self.excel_names_by_category = names_by_category
            self.excel_country_by_category = countries_by_category
            cats = sorted(self.excel_names_by_category.keys())
            self.category_cb.blockSignals(True)
            self.category_cb.clear()
            self.category_cb.addItems(cats)
            self.category_cb.blockSignals(False)
            if cats:
                if self.current_category not in cats:
                    self.current_category = cats[0]
                self.category_cb.setCurrentText(self.current_category)
            self._apply_selected_category_meta()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", str(e))

    def export_csv(self):
        path, _ = QFileDialog.getSaveFileName(self, "Сохранить CSV", "", "CSV (*.csv)")
        if not path:
            return
        try:
            all_splits = set()
            for run in self.model.runs.values():
                all_splits.update(run.split_ids())
            all_splits_sorted = sorted(all_splits, key=split_sort_key)
            cols = ["run", "bib", "name"] + [f"S{sid}" for sid in all_splits_sorted] + ["finish", "status"]
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f, delimiter=";")
                w.writerow(cols)
                for run in self.model.runs.values():
                    for bib in run.bib_order:
                        if bib not in run.athletes:
                            continue
                        a = run.athletes[bib]
                        row = [run.key, a.bib, a.name]
                        for sid in all_splits_sorted:
                            row.append(fmt_time(a.splits.get(str(sid))))
                        row.append(fmt_time(a.finish))
                        row.append(a.status)
                        w.writerow(row)
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", str(e))

    def _apply_evt(self, evt: Dict[str, Any]):
        evt = dict(evt or {})
        evt["category"] = self.current_category
        evt2 = self._sanitize_evt(evt)
        if evt2 is None:
            self._evt_skipped += 1
            self._append_log("WARN: skipped malformed event")
            return

        run_key = self.model.apply(evt2)
        self._send_stats_if_needed(evt2, run_key)
        self._append_log(evt2.get("raw", ""))
        if run_key and str(evt2.get("type") or "") == "new_run":
            self.selected_run_key = run_key
        elif run_key and self.selected_run_key is None:
            self.selected_run_key = run_key

        now_m = time.monotonic()
        evt_type = str(evt2.get("type") or "")
        immediate = evt_type in ("new_run", "setup", "start", "finish")
        if immediate or (now_m - self._last_ui_refresh_ts) >= 0.15:
            self._refresh_runs()
            self._refresh_athletes()
            self._last_ui_refresh_ts = now_m
            self._ui_dirty = False
        else:
            self._ui_dirty = True

        if immediate or (now_m - self._last_state_broadcast_ts) >= 0.10:
            self.net.broadcast_state(self._model_to_state())
            self._last_state_broadcast_ts = now_m
        self._evt_processed += 1

    def _send_stats_if_needed(self, evt: Dict[str, Any], run_key: Optional[str]):
        if not self.stats_sender.enabled:
            return
        if str(evt.get("type") or "") != "finish":
            return
        if not run_key:
            return

        run = self.model.runs.get(run_key)
        if not run:
            return

        bib = str(evt.get("bib") or "").strip()
        if not bib:
            return

        athlete = run.athletes.get(bib)
        if not athlete:
            return

        finish_sec = athlete.finish
        if finish_sec is None:
            return

        split_count = last_split_num(athlete)
        checkpoints = split_count + 1
        distance_m = int(checkpoints * DIST_STEP_M)
        created_at = datetime.datetime.now().isoformat(timespec="seconds")
        event_key = f"{run.key}|{bib}|{finish_sec:.3f}|{split_count}|{run.start_time or ''}"
        event_id = hashlib.sha1(event_key.encode("utf-8", errors="ignore")).hexdigest()

        payload = {
            "event_id": event_id,
            "source": "quantum-server-qt",
            "created_at": created_at,
            "category": str(run.category or self.current_category or "Без категории"),
            "run_key": str(run.key),
            "run_started_at_text": str(run.start_time or ""),
            "distance_m": distance_m,
            "bib": bib,
            "name": str(athlete.name or bib),
            "country": str(athlete.country or ""),
            "finish_sec": float(finish_sec),
            "finish_text": fmt_time(finish_sec),
            "status": str(athlete.status or "финиш"),
            "splits": dict(athlete.splits or {}),
        }
        self.stats_sender.send(payload)

    def _run_sort_key(self, k: str) -> Tuple[int, int, str]:
        m = re.match(r"^(\d+)-(\d+)$", str(k))
        if m:
            return (int(m.group(1)), int(m.group(2)), str(k))
        return (10**9, 10**9, str(k))

    def _refresh_runs(self):
        prev_sel = self.selected_run_key
        self.runs_table.setRowCount(0)
        flt = self.run_filter.text().strip().lower()

        keys = sorted(self.model.runs.keys(), key=self._run_sort_key)
        for run_key in keys:
            run = self.model.runs[run_key]
            hay = f"{run.key} {run.start_time or ''} {run.total_count()} {run.finished_count()} {run.category}".lower()
            if flt and flt not in hay:
                continue
            r = self.runs_table.rowCount()
            self.runs_table.insertRow(r)
            vals = [run.key, run.category, run.start_time or "", str(run.total_count()), str(run.finished_count())]
            for cidx, v in enumerate(vals):
                it = QTableWidgetItem(v)
                if cidx in (0, 2, 3, 4):
                    it.setTextAlignment(Qt.AlignCenter)
                self.runs_table.setItem(r, cidx, it)
            if run.key == prev_sel:
                self.runs_table.selectRow(r)

    def _on_run_selected(self, row: int, _col: int):
        it = self.runs_table.item(row, 0)
        if not it:
            return
        self.selected_run_key = it.text().strip()
        self._refresh_athletes()

    def _refresh_athletes(self):
        run = self.model.runs.get(self.selected_run_key) if self.selected_run_key else None
        if not run:
            self.run_info.setText("—")
            self.ath_table.setRowCount(0)
            base = ["№", "Имя"]
            if self.show_dist_cb.isChecked():
                base.append("Дист.")
            base += ["Финиш", "Статус"]
            self.ath_table.setColumnCount(len(base))
            self.ath_table.setHorizontalHeaderLabels(base)
            return

        self.run_info.setText(f"{run.key}   кат: {run.category or '—'}   старт: {run.start_time or '—'}   участников: {run.total_count()}   финиш: {run.finished_count()}")

        split_ids = run.split_ids()
        headers = ["№", "Имя"]
        if self.show_dist_cb.isChecked():
            headers.append("Дист.")
        headers += [f"S{sid}" for sid in split_ids] + ["Финиш", "Статус"]
        self.ath_table.setColumnCount(len(headers))
        self.ath_table.setHorizontalHeaderLabels(headers)

        split_start_col = 2 + (1 if self.show_dist_cb.isChecked() else 0)
        finish_col = split_start_col + len(split_ids)

        for i, sid in enumerate(split_ids):
            ci = split_start_col + i
            item = self.ath_table.horizontalHeaderItem(ci)
            if item is None:
                continue
            try:
                if int(str(sid)) % 2 == 0:
                    item.setBackground(QColor("#4a391f"))
                    item.setForeground(QColor("#ffe1a3"))
                    item.setText(f"S{sid} •")
            except Exception:
                pass

        hi_finish = self.ath_table.horizontalHeaderItem(finish_col)
        if hi_finish is not None:
            hi_finish.setText("Финиш •")
            hi_finish.setBackground(QColor("#4a391f"))
            hi_finish.setForeground(QColor("#ffe1a3"))

        self.ath_table.setRowCount(0)
        even_split_cols = []
        for i, sid in enumerate(split_ids):
            try:
                if int(str(sid)) % 2 == 0:
                    even_split_cols.append(split_start_col + i)
            except Exception:
                pass

        for bib in run.bib_order:
            if bib not in run.athletes:
                continue
            a = run.athletes[bib]
            r = self.ath_table.rowCount()
            self.ath_table.insertRow(r)
            vals: List[str] = [a.bib, a.name]
            if self.show_dist_cb.isChecked():
                checkpoints = len(a.splits) + (1 if a.finish is not None else 0)
                vals.append(f"{int(checkpoints * DIST_STEP_M)}м" if checkpoints > 0 else "")
            vals += [fmt_time(a.splits.get(str(sid))) for sid in split_ids] + [fmt_time(a.finish), a.status]
            for cidx, v in enumerate(vals):
                it = QTableWidgetItem(v)
                if cidx != 1:
                    it.setTextAlignment(Qt.AlignCenter)
                if cidx >= split_start_col and cidx <= split_start_col + len(split_ids):
                    f = it.font()
                    f.setFamily("Consolas")
                    f.setPointSize(13)
                    it.setFont(f)
                if cidx in even_split_cols:
                    it.setBackground(QColor("#1D4031"))
                if cidx == finish_col:
                    it.setBackground(QColor("#1D4031"))
                self.ath_table.setItem(r, cidx, it)

        self.ath_table.resizeColumnsToContents()
        key = run.key
        prev_cnt = self._last_split_count_by_run.get(key, 0)
        cur_cnt = len(split_ids)
        self._last_split_count_by_run[key] = cur_cnt
        if cur_cnt > prev_cnt and cur_cnt > 0:
            self.ath_table.horizontalScrollBar().setValue(self.ath_table.horizontalScrollBar().maximum())

    def _athlete_display_live(self, run, a):
        if not run or not a:
            return None
        if run.start_mono is None:
            return None
        if a.finish is not None:
            return a.finish
        if a.is_paused():
            return a.pause_value
        return max(0.0, time.monotonic() - run.start_mono)

    def _tick_live(self):
        key = self.selected_run_key or self.model.current_key
        run = self.model.runs.get(key) if key else None
        if not run:
            self.live_run.setText("Заезд: —")
            self.live_left.setText("")
            self.live_right.setText("")
            payload = {"ts": 0, "run": "", "left": {}, "right": {}}
            with self._obs_lock:
                self._obs_payload = payload
            try:
                _atomic_write_text(self.obs_json, json.dumps(payload, ensure_ascii=False))
                self.live_tcp.broadcast(payload)
            except Exception:
                pass
            return

        self.live_run.setText(f"Заезд: {run.key}")
        b1 = run.active_bibs[0] if len(run.active_bibs) > 0 else ""
        b2 = run.active_bibs[1] if len(run.active_bibs) > 1 else ""
        a1 = run.athletes.get(b1) if b1 else None
        a2 = run.athletes.get(b2) if b2 else None
        t1 = self._athlete_display_live(run, a1) if a1 else None
        t2 = self._athlete_display_live(run, a2) if a2 else None
        self.live_left.setText(f"{b1}  {a1.name if a1 else ''}\n{fmt_live(t1) if t1 is not None else ''}")
        self.live_right.setText(f"{b2}  {a2.name if a2 else ''}\n{fmt_live(t2) if t2 is not None else ''}")

        start_epoch = None
        if run.start_mono is not None:
            start_epoch = time.time() - (time.monotonic() - run.start_mono)

        def pack_live(a, bib):
            if not a or not bib:
                return {}
            country = (a.country or "").strip()
            if a.finish is not None:
                phase = "finish"; disp = float(a.finish)
            elif a.is_paused():
                phase = "split"; disp = float(a.pause_value or 0.0)
            elif run.start_mono is not None:
                phase = "live"; disp = max(0.0, time.monotonic() - run.start_mono)
            else:
                phase = "ready"; disp = 0.0

            checkpoints = last_split_num(a) + (1 if a.finish is not None else 0)
            dist_m = int(checkpoints * DIST_STEP_M)
            paused_until_epoch = None if (a.pause_until in (0, None) or a.finish is not None or a.pause_until == float("inf")) else (time.time() + (a.pause_until - time.monotonic()))
            return {
                "bib": bib,
                "name": (a.name or "").strip(),
                "country": country,
                "cc": cc_short(country),
                "flag": flag_for_excel_value(country),
                "start_epoch": start_epoch,
                "paused_until_epoch": paused_until_epoch,
                "paused_value": a.pause_value,
                "phase": phase,
                "time": disp,
                "time_text": fmt_live(disp),
                "last_split": (last_split_num(a) if last_split_num(a) > 0 else None),
                "distance_m": dist_m,
                "distance_text": (f"{dist_m} м" if self.show_dist_cb.isChecked() else ""),
                "finish": a.finish,
            }

        payload = {
            "ts": time.time(),
            "run": run.key,
            "distance_step_m": DIST_STEP_M,
            "distance_enabled": bool(self.show_dist_cb.isChecked()),
            "left": pack_live(a1, b1),
            "right": pack_live(a2, b2),
        }
        try:
            self.live_tcp.broadcast(payload)
        except Exception:
            pass
        with self._obs_lock:
            self._obs_payload = payload
        try:
            _atomic_write_text(self.obs_json, json.dumps(payload, ensure_ascii=False))
        except Exception:
            pass

    def _pump(self):
        processed_this_tick = 0
        max_per_tick = 250
        try:
            while processed_this_tick < max_per_tick:
                item = self.q.get_nowait()
                processed_this_tick += 1
                try:
                    if item["kind"] == "evt":
                        self._apply_evt(item["data"])
                    elif item["kind"] == "err":
                        self._append_log("ERROR: " + str(item.get("data") or ""))
                        self._set_status("Ошибка", False)
                except Exception as e:
                    self._evt_skipped += 1
                    self._append_trace("_pump item", e, item if isinstance(item, dict) else None)
        except queue.Empty:
            pass

        if self._ui_dirty:
            now_m = time.monotonic()
            if (now_m - self._last_ui_refresh_ts) >= 0.15:
                try:
                    self._refresh_runs()
                    self._refresh_athletes()
                    self._last_ui_refresh_ts = now_m
                    self._ui_dirty = False
                except Exception as e:
                    self._render_errors += 1
                    self._append_trace("_refresh_ui_deferred", e)

        try:
            self._tick_live()
        except Exception as e:
            self._render_errors += 1
            self._append_trace("_tick_live", e)

        total_diag = self._evt_processed + self._evt_skipped + self._render_errors
        if total_diag > 0 and total_diag % 200 == 0 and total_diag != self._last_diag_total:
            self._last_diag_total = total_diag
            self.statusBar().showMessage(
                f"events: ok={self._evt_processed} skipped={self._evt_skipped} render_err={self._render_errors}"
            )

    def closeEvent(self, event):
        try:
            self.stats_sender.stop()
        except Exception:
            pass
        try:
            self.live_tcp.stop()
        except Exception:
            pass
        try:
            with self._obs_lock:
                self._obs_payload = {"ts": 0, "run": "", "left": {}, "right": {}}
            self.overlay_http.stop()
        except Exception:
            pass
        try:
            self.disconnect()
        except Exception:
            pass
        try:
            self.net.stop()
        except Exception:
            pass
        super().closeEvent(event)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=9876)
    ap.add_argument("--com")
    ap.add_argument("--baud", type=int, default=9600)
    ap.add_argument("--replay")
    ap.add_argument("--stats-endpoint", default=os.getenv("SWISS_STATS_ENDPOINT", "http://127.0.0.1:18080/ingest/result"))
    args = ap.parse_args()

    app = QApplication(sys.argv)
    w = QtServerApp(args.listen, args.port, stats_endpoint=args.stats_endpoint)
    if args.replay:
        w.stop_evt.clear()
        w.reader = ReaderThread(w.q, w.stop_evt, replay_path=args.replay)
        w.reader.start()
        w._set_status("Режим файла", True)
    elif args.com:
        w.port_cb.setCurrentText(args.com)
        w.baud_cb.setCurrentText(str(args.baud))
        w.connect_serial()

    w.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
