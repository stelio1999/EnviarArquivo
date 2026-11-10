# server.py - FileBridge Web (Correção Definitiva para Render)
import asyncio
import json
import os
import time
from typing import Dict
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse, Response
import uvicorn

app = FastAPI()

# ─────────────────────────────────────────────────────────────────────────────
# GERENCIADOR DE CONEXÕES
# ─────────────────────────────────────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, dict] = {}

    async def broadcast_peers(self):
        peers = list(self.active_connections.keys())
        msg = json.dumps({"type": "peers_update", "peers": peers})
        for data in list(self.active_connections.values()):
            try:
                await data["ws"].send_text(msg)
            except:
                pass

    async def send_to_peer(self, target_id: str, message: str) -> bool:
        if target_id in self.active_connections:
            try:
                await self.active_connections[target_id]["ws"].send_text(message)
                return True
            except:
                return False
        return False

    def register(self, device_id: str, ws: WebSocket):
        if device_id in self.active_connections:
            old_ws = self.active_connections[device_id]["ws"]
            try: asyncio.create_task(old_ws.close())
            except: pass
        self.active_connections[device_id] = {"ws": ws, "last_seen": time.time()}
        print(f"✅ Registrado: {device_id} (Total: {len(self.active_connections)})")

    def disconnect(self, device_id: str):
        if device_id in self.active_connections:
            del self.active_connections[device_id]
            print(f"❌ Desconectado: {device_id} (Total: {len(self.active_connections)})")
            asyncio.create_task(self.broadcast_peers())

    def cleanup_stale(self):
        now = time.time()
        removed = []
        for did, data in list(self.active_connections.items()):
            if now - data["last_seen"] > 60:
                removed.append(did)
                del self.active_connections[did]
        if removed:
            print(f"🧹 Limpando IDs órfãos: {removed}")
            asyncio.create_task(self.broadcast_peers())

manager = ConnectionManager()

# ─────────────────────────────────────────────────────────────────────────────
# MIDDLEWARE PARA GARANTIR A RAIZ (SOLUÇÃO DO 426)
# ─────────────────────────────────────────────────────────────────────────────
@app.middleware("http")
async def force_html_on_root(request: Request, call_next):
    # Se a requisição for para a raiz ("/"), retorna o HTML imediatamente
    if request.url.path == "/":
        return HTMLResponse(HTML_CONTENT)
    # Para qualquer outra rota, segue o fluxo normal
    return await call_next(request)

# ─────────────────────────────────────────────────────────────────────────────
# INTERFACE WEB (HTML + CSS + JS + FOOTER)
# ─────────────────────────────────────────────────────────────────────────────
HTML_CONTENT = """
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>FileBridge · Web P2P</title>
    <style>
        :root {
            --bg-top: #0A1A2F;
            --bg-bottom: #12294A;
            --glass: rgba(255, 255, 255, 0.12);
            --glass-strong: rgba(255, 255, 255, 0.2);
            --stroke: rgba(255, 255, 255, 0.25);
            --primary: #1E5FA8;
            --mz-green: #009639;
            --mz-yellow: #FFCE00;
            --mz-red: #C8102E;
            --text: #FFFFFF;
            --text-dim: #B8C6D9;
        }
        * { box-sizing: border-box; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
        body {
            margin: 0; padding: 20px; min-height: 100vh;
            background: linear-gradient(135deg, var(--bg-top), var(--bg-bottom));
            color: var(--text); display: flex; flex-direction: column; align-items: center;
        }
        .container { width: 100%; max-width: 1000px; display: flex; flex-direction: column; gap: 20px; flex: 1; }
        .glass-panel {
            background: var(--glass); border: 1px solid var(--stroke);
            border-radius: 20px; padding: 24px;
            backdrop-filter: blur(20px); -webkit-backdrop-filter: blur(20px);
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.2);
        }
        .header { display: flex; justify-content: space-between; align-items: center; }
        .header h1 { margin: 0; font-size: 24px; display: flex; align-items: center; gap: 10px; }
        .device-id { color: var(--mz-yellow); font-weight: bold; font-size: 18px; background: var(--glass-strong); padding: 8px 16px; border-radius: 10px; border: 1px solid var(--stroke); }
        .main-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
        @media (max-width: 768px) { .main-grid { grid-template-columns: 1fr; } }
        .card h2 { margin: 0 0 16px 0; font-size: 18px; }
        .form-group { display: flex; flex-direction: column; gap: 12px; }
        input[type="text"] {
            width: 100%; padding: 12px 16px; background: var(--glass); border: 1px solid var(--stroke);
            border-radius: 12px; color: var(--text); font-size: 16px; outline: none;
        }
        input[type="text"]::placeholder { color: var(--text-dim); }
        button {
            padding: 12px 24px; border: none; border-radius: 12px; cursor: pointer;
            font-size: 16px; font-weight: 600; color: white; background: var(--primary);
            transition: opacity 0.2s; display: flex; align-items: center; justify-content: center; gap: 8px;
        }
        button:hover { opacity: 0.9; }
        button:disabled { opacity: 0.5; cursor: not-allowed; }
        .progress-container { display: none; flex-direction: column; gap: 8px; margin-top: 16px; }
        .progress-bar { width: 100%; height: 8px; background: var(--glass-strong); border-radius: 4px; overflow: hidden; }
        .progress-fill { height: 100%; background: var(--mz-green); width: 0%; transition: width 0.2s; }
        .progress-text { font-size: 12px; color: var(--text-dim); text-align: right; }
        .peers-list, .inbox-list { display: flex; flex-direction: column; gap: 8px; max-height: 300px; overflow-y: auto; }
        .peer-item, .inbox-item {
            background: var(--glass-strong); padding: 12px; border-radius: 10px;
            font-size: 14px; display: flex; justify-content: space-between; align-items: center; gap: 10px;
        }
        .peer-item.self { border: 1px solid var(--mz-green); }
        .peer-item .badge {
            font-size: 10px; background: var(--mz-green); color: white;
            padding: 2px 8px; border-radius: 6px; margin-left: 6px;
        }
        .inbox-item button { padding: 8px 16px; font-size: 14px; background: var(--mz-green); }
        .status { font-size: 12px; margin-top: 8px; }
        .status.online { color: var(--mz-green); }
        .status.offline { color: var(--mz-red); }

        /* FOOTER */
        .footer {
            margin-top: 24px; padding: 20px 24px;
            background: var(--glass); border: 1px solid var(--stroke);
            border-radius: 20px;
            display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 16px;
            font-size: 13px; color: var(--text-dim);
        }
        .footer .left { display: flex; flex-direction: column; gap: 4px; }
        .footer .left strong { color: var(--text); font-size: 14px; }
        .footer .left span { color: var(--mz-yellow); font-size: 11px; }
        .footer .right { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }
        .footer a {
            color: var(--text); text-decoration: none; transition: color 0.2s;
            display: flex; align-items: center; gap: 6px;
        }
        .footer a:hover { color: var(--mz-green); }
        .footer .btn-portfolio {
            padding: 8px 16px; border-radius: 10px; background: var(--primary); color: white;
            font-size: 13px; font-weight: 600; display: flex; align-items: center; gap: 6px;
        }
        .footer .btn-portfolio:hover { opacity: 0.9; color: white; }
        @media (max-width: 600px) {
            .footer { flex-direction: column; text-align: center; }
            .footer .right { justify-content: center; }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="glass-panel header">
            <h1>🌉 FileBridge <span style="font-size: 12px; color: var(--text-dim); font-weight: normal;">P2P Web</span></h1>
            <div class="device-id" id="myId">Gerando ID...</div>
        </div>

        <div class="main-grid">
            <div class="glass-panel card">
                <h2>Enviar Arquivo</h2>
                <div class="form-group">
                    <input type="text" id="targetId" placeholder="ID do destinatário (ex: AB1234)">
                    <input type="file" id="fileInput" style="display: none;">
                    <button id="btnSelectFile" onclick="document.getElementById('fileInput').click()">
                        📁 Selecionar e Enviar
                    </button>
                </div>
                <div class="progress-container" id="sendProgressContainer">
                    <div class="progress-bar"><div class="progress-fill" id="sendProgressFill"></div></div>
                    <div class="progress-text" id="sendProgressText">0%</div>
                </div>
                <div class="status" id="statusText">Conectando...</div>
            </div>

            <div class="glass-panel card">
                <h2>Recebidos / Peers</h2>
                <div style="display: flex; gap: 10px; margin-bottom: 12px;">
                    <button onclick="switchTab('inbox')" id="tabInbox" style="flex: 1; background: var(--primary);">📥 Recebidos</button>
                    <button onclick="switchTab('peers')" id="tabPeers" style="flex: 1; background: var(--glass-strong); color: var(--text);">🌐 Peers</button>
                </div>
                <div id="inboxView" class="inbox-list">
                    <div style="color: var(--text-dim); font-size: 13px; text-align: center; padding: 20px;">Nenhum arquivo recebido.</div>
                </div>
                <div id="peersView" class="peers-list" style="display: none;">
                    <div style="color: var(--text-dim); font-size: 13px; text-align: center; padding: 20px;">Nenhum peer online.</div>
                </div>
                <div class="progress-container" id="recvProgressContainer" style="margin-top: 16px;">
                    <div class="progress-bar"><div class="progress-fill" id="recvProgressFill" style="background: var(--mz-yellow);"></div></div>
                    <div class="progress-text" id="recvProgressText">0%</div>
                </div>
            </div>
        </div>

        <!-- FOOTER -->
        <div class="footer">
            <div class="left">
                <strong>Stélio Virgulino</strong>
                <span>Full Stack Engineer</span>
            </div>
            <div class="right">
                <a href="mailto:steliobobo52@gmail.com">📧 steliobobo52@gmail.com</a>
                <a href="tel:+258846967721">📱 (+258) 84 696 7721</a>
                <a href="https://portifolio.wuaze.com" target="_blank" class="btn-portfolio">
                    🌐 Meu Portfólio
                </a>
            </div>
        </div>
    </div>

    <script>
        let ws = null;
        let myDeviceId = "";
        let peers = [];
        let receivingFile = null;
        let receivedFiles = [];
        const CHUNK_SIZE = 512 * 1024;
        let pollInterval = null;

        const myIdEl = document.getElementById("myId");
        const statusEl = document.getElementById("statusText");
        const targetInput = document.getElementById("targetId");
        const fileInput = document.getElementById("fileInput");
        const sendProgressContainer = document.getElementById("sendProgressContainer");
        const sendProgressFill = document.getElementById("sendProgressFill");
        const sendProgressText = document.getElementById("sendProgressText");
        const recvProgressContainer = document.getElementById("recvProgressContainer");
        const recvProgressFill = document.getElementById("recvProgressFill");
        const recvProgressText = document.getElementById("recvProgressText");
        const peersView = document.getElementById("peersView");
        const inboxView = document.getElementById("inboxView");

        function getOrCreateDeviceId() {
            let id = localStorage.getItem("filebridge_device_id");
            if (!id || id.length !== 6) {
                const letras = "ABCDEFGHJKLMNPQRSTUVWXYZ";
                id = `${letras[Math.floor(Math.random() * letras.length)]}${letras[Math.floor(Math.random() * letras.length)]}${Math.floor(1000 + Math.random() * 9000)}`;
                localStorage.setItem("filebridge_device_id", id);
            }
            return id;
        }

        function connect() {
            const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
            const wsUrl = `${protocol}//${window.location.host}/ws`;
            ws = new WebSocket(wsUrl);

            ws.onopen = () => {
                myDeviceId = getOrCreateDeviceId();
                myIdEl.textContent = myDeviceId;
                statusEl.textContent = "✅ Conectado ao servidor";
                statusEl.className = "status online";
                ws.send(JSON.stringify({ type: "register", device_id: myDeviceId }));

                if (pollInterval) clearInterval(pollInterval);
                pollInterval = setInterval(() => {
                    if (ws && ws.readyState === WebSocket.OPEN) {
                        ws.send(JSON.stringify({ type: "ping" }));
                    }
                }, 5000);
            };

            ws.onmessage = (event) => {
                try { handleMessage(JSON.parse(event.data)); } catch (e) { console.error(e); }
            };

            ws.onclose = () => {
                statusEl.textContent = "❌ Desconectado. Reconectando...";
                statusEl.className = "status offline";
                if (pollInterval) clearInterval(pollInterval);
                setTimeout(connect, 3000);
            };
        }

        function handleMessage(msg) {
            switch (msg.type) {
                case "peers_update":
                    peers = msg.peers;
                    renderPeers();
                    break;
                case "error": alert("Erro: " + msg.message); break;
                case "delivered":
                    statusEl.textContent = "✅ Arquivo entregue!";
                    statusEl.className = "status online";
                    break;
                case "pong": break;

                case "file_start":
                    receivingFile = { filename: msg.filename, from: msg.from, total: msg.size, chunks: [], receivedBytes: 0 };
                    recvProgressContainer.style.display = "flex";
                    recvProgressFill.style.width = "0%";
                    recvProgressText.textContent = "0%";
                    break;

                case "file_chunk":
                    if (!receivingFile) return;
                    const chunkBytes = base64ToBytes(msg.data);
                    receivingFile.chunks.push(chunkBytes);
                    receivingFile.receivedBytes += chunkBytes.length;
                    const pct = Math.min(100, (receivingFile.receivedBytes / receivingFile.total) * 100);
                    recvProgressFill.style.width = pct + "%";
                    recvProgressText.textContent = Math.round(pct) + "%";
                    break;

                case "file_end":
                    if (!receivingFile) return;
                    const totalLen = receivingFile.chunks.reduce((acc, c) => acc + c.length, 0);
                    const finalBytes = new Uint8Array(totalLen);
                    let offset = 0;
                    for (const c of receivingFile.chunks) { finalBytes.set(c, offset); offset += c.length; }
                    const blob = new Blob([finalBytes], { type: "application/octet-stream" });
                    const fileId = Date.now().toString() + Math.random().toString(36).substring(2, 6);
                    receivedFiles.push({ id: fileId, name: receivingFile.filename, blob, from: receivingFile.from, size: finalBytes.length });
                    addInboxItem(fileId, receivingFile.filename, receivingFile.from, finalBytes.length);
                    receivingFile = null;
                    recvProgressFill.style.width = "100%";
                    recvProgressText.textContent = "100%";
                    setTimeout(() => { recvProgressContainer.style.display = "none"; }, 1500);
                    break;
            }
        }

        function base64ToBytes(b64) {
            const bin = atob(b64);
            const bytes = new Uint8Array(bin.length);
            for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
            return bytes;
        }

        function bytesToBase64(bytes) {
            let bin = "";
            const chunkSize = 0x8000;
            for (let i = 0; i < bytes.length; i += chunkSize) {
                bin += String.fromCharCode.apply(null, bytes.subarray(i, i + chunkSize));
            }
            return btoa(bin);
        }

        function renderPeers() {
            if (peers.length === 0) {
                peersView.innerHTML = '<div style="color: var(--text-dim); font-size: 13px; text-align: center; padding: 20px;">Nenhum peer online.</div>';
                return;
            }
            peersView.innerHTML = peers.map(p => {
                const isSelf = p === myDeviceId;
                return `<div class="peer-item ${isSelf ? 'self' : ''}">
                    <span>${p}${isSelf ? '<span class="badge">você</span>' : ''}</span>
                </div>`;
            }).join("");
        }

        function addInboxItem(fileId, filename, from, size) {
            const placeholder = inboxView.querySelector('div[style*="text-align: center"]');
            if (placeholder) placeholder.remove();
            const item = document.createElement("div");
            item.className = "inbox-item";
            item.innerHTML = `
                <div style="display: flex; flex-direction: column; gap: 4px;">
                    <span>📄 ${filename}</span>
                    <span style="color: var(--text-dim); font-size: 11px;">De: ${from} | ${(size / 1024).toFixed(1)} KB</span>
                </div>
                <button onclick="downloadFile('${fileId}')">⬇️ Baixar</button>
            `;
            inboxView.prepend(item);
        }

        function downloadFile(fileId) {
            const fileData = receivedFiles.find(f => f.id === fileId);
            if (!fileData) return;
            const url = URL.createObjectURL(fileData.blob);
            const a = document.createElement("a");
            a.href = url;
            a.download = fileData.name;
            a.click();
            URL.revokeObjectURL(url);
        }

        function switchTab(tab) {
            const tabInbox = document.getElementById("tabInbox");
            const tabPeers = document.getElementById("tabPeers");
            const inboxView = document.getElementById("inboxView");
            const peersView = document.getElementById("peersView");
            if (tab === "inbox") {
                tabInbox.style.background = "var(--primary)"; tabInbox.style.color = "white";
                tabPeers.style.background = "var(--glass-strong)"; tabPeers.style.color = "var(--text)";
                inboxView.style.display = "flex"; peersView.style.display = "none";
            } else {
                tabPeers.style.background = "var(--primary)"; tabPeers.style.color = "white";
                tabInbox.style.background = "var(--glass-strong)"; tabInbox.style.color = "var(--text)";
                inboxView.style.display = "none"; peersView.style.display = "flex";
            }
        }

        fileInput.addEventListener("change", async (e) => {
            const targetId = targetInput.value.trim().toUpperCase();
            if (!targetId) { alert("Digite o ID do destinatário."); return; }
            const file = e.target.files[0];
            if (!file) return;
            if (file.size > 50 * 1024 * 1024) { alert("Limite de 50 MB excedido."); return; }
            if (!ws || ws.readyState !== WebSocket.OPEN) { alert("Sem conexão."); return; }

            const transferId = crypto.randomUUID().slice(0, 12);
            sendProgressContainer.style.display = "flex";
            sendProgressFill.style.width = "0%";
            sendProgressText.textContent = "0%";

            ws.send(JSON.stringify({ type: "file_start", transfer_id: transferId, target: targetId, from: myDeviceId, filename: file.name, size: file.size }));

            const reader = new FileReader();
            let offset = 0;
            const readChunk = () => {
                const slice = file.slice(offset, offset + CHUNK_SIZE);
                reader.onload = (event) => {
                    const bytes = new Uint8Array(event.target.result);
                    const b64 = bytesToBase64(bytes);
                    ws.send(JSON.stringify({ type: "file_chunk", transfer_id: transferId, target: targetId, data: b64 }));
                    offset += CHUNK_SIZE;
                    const pct = Math.min(100, (offset / file.size) * 100);
                    sendProgressFill.style.width = pct + "%";
                    sendProgressText.textContent = Math.round(pct) + "%";
                    if (offset < file.size) { setTimeout(readChunk, 10); } 
                    else {
                        ws.send(JSON.stringify({ type: "file_end", transfer_id: transferId, target: targetId }));
                        setTimeout(() => {
                            sendProgressContainer.style.display = "none";
                            statusEl.textContent = "✅ Envio concluído!";
                            statusEl.className = "status online";
                        }, 1000);
                    }
                };
                reader.readAsArrayBuffer(slice);
            };
            readChunk();
            fileInput.value = "";
        });

        connect();
    </script>
</body>
</html>
"""

# ─────────────────────────────────────────────────────────────────────────────
# ROTA WEBSOCKET
# ─────────────────────────────────────────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    device_id = None
    try:
        await websocket.accept()
        raw = await websocket.receive_text()
        msg = json.loads(raw)
        if msg.get("type") == "register":
            device_id = msg.get("device_id")
            manager.register(device_id, websocket)
            await manager.broadcast_peers()

        while True:
            raw = await websocket.receive_text()
            msg = json.loads(raw)
            typ = msg.get("type")
            if typ == "ping":
                if device_id in manager.active_connections:
                    manager.active_connections[device_id]["last_seen"] = time.time()
                await websocket.send_text(json.dumps({"type": "pong"}))
                await manager.broadcast_peers()
            elif typ in ["file_start", "file_chunk", "file_end"]:
                target = msg.get("target")
                if target:
                    await manager.send_to_peer(target, raw)
                    if typ == "file_end":
                        await websocket.send_text(json.dumps({"type": "delivered", "transfer_id": msg.get("transfer_id")}))
                else:
                    if typ == "file_start":
                        await websocket.send_text(json.dumps({"type": "error", "message": f"Peer {target} offline"}))
    except WebSocketDisconnect:
        if device_id: manager.disconnect(device_id)
    except Exception as e:
        print(f"Erro: {e}")
        if device_id: manager.disconnect(device_id)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(cleanup_loop())

async def cleanup_loop():
    while True:
        await asyncio.sleep(30)
        manager.cleanup_stale()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)