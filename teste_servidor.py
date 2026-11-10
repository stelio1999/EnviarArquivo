# teste_servidor.py
import asyncio
import websockets
import json

async def testar():
    uri = "wss://enviararquivo.onrender.com"
    async with websockets.connect(uri) as ws:
        print("Conectado!")
        await ws.send(json.dumps({"type": "register", "device_id": "TESTE1"}))
        async for msg in ws:
            print(f"Recebido: {msg}")
            break

asyncio.run(testar())