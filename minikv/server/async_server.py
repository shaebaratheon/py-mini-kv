"""AsyncIO TCP High-Performance RESP Server for MiniKV."""

import asyncio
import os
from typing import Dict, Any, Optional
from minikv.storage.engine import StorageEngine
from minikv.server.protocol import RESPParser

class AsyncMiniKVServer:
    def __init__(self, host: str = "127.0.0.1", port: int = 6389, data_dir: str = "/tmp/minikv_data"):
        self.host = host
        self.port = port
        self.engine = StorageEngine(data_dir=data_dir)
        self.server: Optional[asyncio.AbstractServer] = None

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        buffer = b""
        while True:
            data = await reader.read(4096)
            if not data:
                break
            buffer += data
            while buffer:
                cmd_parts, buffer = RESPParser.parse_command(buffer)
                if not cmd_parts:
                    break
                resp = self._execute_command(cmd_parts)
                writer.write(resp)
                await writer.drain()
        writer.close()
        await writer.wait_closed()

    def _execute_command(self, parts: list) -> bytes:
        if not parts:
            return RESPParser.serialize_error("empty command")
        cmd = parts[0].upper()
        if cmd == "PING":
            return RESPParser.serialize_simple_string("PONG")
        elif cmd == "SET":
            if len(parts) < 3:
                return RESPParser.serialize_error("wrong number of arguments for 'set' command")
            self.engine.put(parts[1], parts[2])
            return RESPParser.serialize_simple_string("OK")
        elif cmd == "GET":
            if len(parts) != 2:
                return RESPParser.serialize_error("wrong number of arguments for 'get' command")
            val = self.engine.get(parts[1])
            return RESPParser.serialize_bulk_string(val)
        elif cmd == "DEL":
            if len(parts) != 2:
                return RESPParser.serialize_error("wrong number of arguments for 'del' command")
            ok = self.engine.delete(parts[1])
            return RESPParser.serialize_integer(1 if ok else 0)
        elif cmd == "SCAN":
            prefix = parts[1] if len(parts) > 1 else ""
            res = self.engine.scan(prefix=prefix)
            flat = []
            for k, v in res:
                flat.append(k)
                flat.append(v)
            return RESPParser.serialize_array(flat)
        else:
            return RESPParser.serialize_error(f"unknown command '{cmd}'")

    async def start(self):
        self.server = await asyncio.start_server(self.handle_client, self.host, self.port)

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        self.engine.close()
