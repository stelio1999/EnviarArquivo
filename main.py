# filebridge/main.py
import asyncio
import base64
import json
import secrets
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
import flet as ft
import websockets

# CONFIG
BASE_DIR = Path(__file__).parent.resolve()
DATA_DIR = BASE_DIR / "data"
UPLOADS_DIR = DATA_DIR / "uploads"
for p in (DATA_DIR, UPLOADS_DIR): p.mkdir(parents=True, exist_ok=True)
RELAY_URL = "wss://enviararquivo.onrender.com"

COLOR_BG_TOP = "#0A1A2F"
COLOR_GLASS = "#1FFFFFFF"
COLOR_GLASS_STRONG = "#33FFFFFF"
COLOR_STROKE = "#40FFFFFF"
COLOR_PRIMARY = "#1E5FA8"
COLOR_ACCENT_MZ_GREEN = "#009639"
COLOR_ACCENT_MZ_YELLOW = "#FFCE00"
COLOR_ACCENT_MZ_RED = "#C8102E"
COLOR_TEXT = "#FFFFFF"
COLOR_TEXT_DIM = "#B8C6D9"
CHUNK_SIZE = 512 * 1024

def now_iso(): return datetime.now().strftime("%H:%M:%S")
def human_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024: return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"

def generate_device_id() -> str:
    return f"{secrets.choice('ABCDEFGHJKLMNPQRSTUVWXYZ')}{secrets.choice('ABCDEFGHJKLMNPQRSTUVWXYZ')}{secrets.randbelow(9000) + 1000}"

def load_or_create_config() -> dict:
    cfg_path = BASE_DIR / "config.json"
    if cfg_path.exists():
        try: return json.loads(cfg_path.read_text(encoding="utf-8"))
        except: pass
    cfg = {"device_id": generate_device_id()}
    cfg_path.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
    return cfg

class CloudClient:
    def __init__(self, device_id: str, on_message):
        self.device_id = device_id
        self.on_message = on_message
        self.ws = None
        self._loop = None
        self._connected = False

    def connect(self, uri: str):
        threading.Thread(target=self._run, args=(uri,), daemon=True).start()

    def _run(self, uri):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._main(uri))

    async def _main(self, uri):
        while True:
            try:
                async with websockets.connect(uri, max_size=100*1024*1024, ping_interval=15) as ws:
                    self.ws, self._connected = ws, True
                    await ws.send(json.dumps({"type": "register", "device_id": self.device_id}))
                    async for raw in ws:
                        try: self.on_message(json.loads(raw))
                        except Exception as e: print(f"Erro msg: {e}")
            except Exception:
                self._connected = False
                await asyncio.sleep(3)

    async def send_async(self, payload: dict):
        if self.ws and self._connected:
            await self.ws.send(json.dumps(payload))

    def send(self, payload: dict):
        if self._loop and self._connected:
            asyncio.run_coroutine_threadsafe(self.send_async(payload), self._loop)

class FileBridgeApp:
    def __init__(self):
        self.cfg = load_or_create_config()
        self.device_id = self.cfg["device_id"]
        self.client = CloudClient(self.device_id, self._on_message)
        self.peers, self.inbox = [], []
        self.page = None
        self.progress = 0.0
        self.receiving_file = {}

    def start_services(self):
        self.client.connect(RELAY_URL)
        time.sleep(1)

    def _on_message(self, msg: dict):
        typ = msg.get("type")
        if typ == "peers_update":
            self.peers = msg.get("peers", [])
            self._update_peers_ui()
        elif typ == "error":
            self._toast(msg.get("message", "Erro"), error=True)
        elif typ == "delivered":
            self._toast("✅ Arquivo entregue!")
        elif typ == "file_start":
            self.receiving_file = {"filename": msg["filename"], "from": msg["from"], "transfer_id": msg["transfer_id"], "data": b"", "total": msg["size"]}
            self._update_progress_ui(0, True)
        elif typ == "file_chunk":
            if self.receiving_file:
                chunk = base64.b64decode(msg["data"])
                self.receiving_file["data"] += chunk
                pct = len(self.receiving_file["data"]) / max(self.receiving_file["total"], 1)
                self._update_progress_ui(pct, True)
        elif typ == "file_end":
            if self.receiving_file:
                out_path = UPLOADS_DIR / f"{self.receiving_file['transfer_id']}_{self.receiving_file['filename']}"
                out_path.write_bytes(self.receiving_file["data"])
                self.inbox.insert(0, {"filename": self.receiving_file["filename"], "size": len(self.receiving_file["data"]), "from": self.receiving_file["from"], "at": now_iso()})
                self.receiving_file = {}
                self._update_progress_ui(1.0, True)
                self._update_inbox_ui()
                self._toast("📥 Arquivo recebido!")
                if self.page: self.page.run_task(self._hide_progress_later)

    async def _hide_progress_later(self):
        await asyncio.sleep(1.5)
        self._update_progress_ui(0, False)

    def _update_progress_ui(self, val: float, visible: bool):
        if not hasattr(self, 'progress_bar') or self.progress_bar is None: return
        try:
            self.progress_bar.value = max(0.0, min(1.0, val))
            self.progress_text.value = f"{int(val * 100)}%"
            self.progress_container.visible = visible
            if self.page: self.page.update()
        except Exception as e: print(f"Erro UI: {e}")

    async def _send_chunks(self, target: str, path: Path):
        tid = uuid.uuid4().hex[:12]
        size = path.stat().st_size
        total_chunks = (size + CHUNK_SIZE - 1) // CHUNK_SIZE
        self.client.send({"type": "file_start", "transfer_id": tid, "target": target, "from": self.device_id, "filename": path.name, "size": size})
        with open(path, "rb") as f:
            for i in range(total_chunks):
                chunk = f.read(CHUNK_SIZE)
                b64 = base64.b64encode(chunk).decode()
                self.client.send({"type": "file_chunk", "target": target, "transfer_id": tid, "data": b64})
                self._update_progress_ui((i + 1) / total_chunks, True)
                await asyncio.sleep(0.02)
        self.client.send({"type": "file_end", "target": target, "transfer_id": tid})
        self._toast("✅ Envio concluído!")

    def _safe_ui_refresh(self):
        if self.page:
            try: self.page.update()
            except: pass

    def _toast(self, text: str, error: bool = False):
        if not self.page: return
        try:
            sb = ft.SnackBar(content=ft.Text(text, color="white"), bgcolor=COLOR_ACCENT_MZ_RED if error else COLOR_ACCENT_MZ_GREEN)
            self.page.show_dialog(sb)
        except: pass

    def build_ui(self, page: ft.Page):
        self.page = page
        page.title = "FileBridge · P2P"
        page.theme_mode = ft.ThemeMode.DARK
        page.bgcolor = COLOR_BG_TOP
        page.window.width = 1000
        page.window.height = 750
        header = ft.Container(content=ft.Row([ft.Icon(ft.Icons.CLOUD, color=COLOR_TEXT, size=26), ft.Text("FileBridge", size=22, weight=ft.FontWeight.W_700, color=COLOR_TEXT), ft.Container(expand=True), ft.Text(f"ID: {self.device_id}", color=COLOR_ACCENT_MZ_YELLOW, size=16, weight=ft.FontWeight.BOLD, selectable=True)]), padding=20, bgcolor=COLOR_GLASS, border_radius=22)
        self.target_field = ft.TextField(label="ID do destinatário", hint_text="Ex: AB1234", filled=True, fill_color=COLOR_GLASS, border=ft.OutlineInputBorder(border_radius=12, side=ft.BorderSide(1, COLOR_STROKE)))
        send_btn = ft.Button("Selecionar e Enviar", icon=ft.Icons.SEND, on_click=self._pick_and_send)
        self.progress_bar = ft.ProgressBar(value=0, color=COLOR_ACCENT_MZ_GREEN, bgcolor=COLOR_GLASS_STRONG)
        self.progress_text = ft.Text("0%", color=COLOR_TEXT, size=12)
        self.progress_container = ft.Container(content=ft.Column([self.progress_bar, self.progress_text]), visible=False, margin=ft.Margin.only(top=10))
        self.peers_list = ft.Column(scroll=ft.ScrollMode.AUTO, expand=True)
        self.inbox_list = ft.Column(scroll=ft.ScrollMode.AUTO, expand=True)
        left = ft.Container(content=ft.Column([ft.Text("Enviar", size=20, weight="bold"), self.target_field, send_btn, self.progress_container]), padding=24, bgcolor=COLOR_GLASS, expand=1, border_radius=22)
        right = ft.Container(content=ft.Tabs(length=2, selected_index=0, expand=True, content=ft.Column([ft.TabBar(tabs=[ft.Tab(label="Recebidos"), ft.Tab(label="Peers Online")]), ft.TabBarView(expand=True, controls=[self.inbox_list, self.peers_list])])), padding=16, bgcolor=COLOR_GLASS, expand=1, border_radius=22)
        page.add(ft.Column([header, ft.Row([left, right], expand=True, spacing=20)], expand=True, spacing=20))
        self._update_peers_ui()
        self._update_inbox_ui()

    def _update_peers_ui(self):
        if not hasattr(self, 'peers_list') or self.peers_list is None: return
        self.peers_list.controls.clear()
        outros = [p for p in self.peers if p != self.device_id]
        if not outros: self.peers_list.controls.append(ft.Text("Nenhum peer online.", color=COLOR_TEXT_DIM))
        for p in outros:
            self.peers_list.controls.append(ft.Container(content=ft.Text(p, color=COLOR_TEXT, weight="bold"), padding=10, bgcolor=COLOR_GLASS_STRONG, border_radius=8))
        self._safe_ui_refresh()

    def _update_inbox_ui(self):
        if not hasattr(self, 'inbox_list') or self.inbox_list is None: return
        self.inbox_list.controls.clear()
        if not self.inbox: self.inbox_list.controls.append(ft.Text("Nenhum arquivo recebido.", color=COLOR_TEXT_DIM))
        for item in self.inbox:
            self.inbox_list.controls.append(ft.Container(content=ft.Column([ft.Text(item["filename"], weight="bold", color=COLOR_TEXT), ft.Text(f"De: {item['from']} às {item['at']} | {human_size(item['size'])}", size=12, color=COLOR_TEXT_DIM)]), padding=10, bgcolor=COLOR_GLASS_STRONG, border_radius=8))
        self._safe_ui_refresh()

    async def _pick_and_send(self, e):
        target = (self.target_field.value or "").strip().upper()
        if not target: return self._toast("Digite o ID", error=True)
        files = await ft.FilePicker().pick_files()
        if files:
            path = Path(files[0].path)
            self._update_progress_ui(0, True)
            asyncio.create_task(self._send_chunks(target, path))

def main(page: ft.Page):
    app = FileBridgeApp()
    app.start_services()
    app.build_ui(page)

if __name__ == "__main__":
    ft.run(main)