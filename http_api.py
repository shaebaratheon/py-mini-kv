"""
HTTP REST and PubSub controller interface for MiniKV.
Provides lightweight JSON-over-HTTP endpoints without third-party web frameworks.
"""

import json
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional, Dict, Any


class MiniKVRequestHandler(BaseHTTPRequestHandler):
    """REST API handler supporting GET, PUT, POST, DELETE."""
    store_engine = None  # Injected store instance

    def _send_json_response(self, status_code: int, payload: Dict[str, Any]):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path_parts = parsed.path.strip("/").split("/")

        if len(path_parts) == 2 and path_parts[0] == "keys":
            key = urllib.parse.unquote(path_parts[1])
            val = self.store_engine.get(key)
            if val is not None:
                self._send_json_response(200, {"key": key, "value": val, "found": True})
            else:
                self._send_json_response(404, {"key": key, "error": "Not Found", "found": False})
        elif parsed.path == "/health":
            self._send_json_response(200, {"status": "UP", "engine": "MiniKV-v2"})
        elif parsed.path == "/metrics":
            self._send_json_response(200, {"keys_count": len(getattr(self.store_engine, "data", {}))})
        else:
            self._send_json_response(400, {"error": "Invalid API path"})

    def do_PUT(self):
        parsed = urllib.parse.urlparse(self.path)
        path_parts = parsed.path.strip("/").split("/")

        if len(path_parts) == 2 and path_parts[0] == "keys":
            key = urllib.parse.unquote(path_parts[1])
            content_len = int(self.headers.get("Content-Length", 0))
            if content_len == 0:
                self._send_json_response(400, {"error": "Empty payload"})
                return

            body = self.rfile.read(content_len).decode("utf-8")
            try:
                data = json.loads(body)
                val = data.get("value")
                ttl = data.get("ttl_seconds")
                self.store_engine.set(key, val, ttl)
                self._send_json_response(200, {"status": "OK", "key": key, "value": val})
            except Exception as e:
                self._send_json_response(500, {"error": str(e)})
        else:
            self._send_json_response(400, {"error": "Invalid PUT route"})

    def do_DELETE(self):
        parsed = urllib.parse.urlparse(self.path)
        path_parts = parsed.path.strip("/").split("/")

        if len(path_parts) == 2 and path_parts[0] == "keys":
            key = urllib.parse.unquote(path_parts[1])
            success = self.store_engine.delete(key)
            if success:
                self._send_json_response(200, {"status": "DELETED", "key": key})
            else:
                self._send_json_response(404, {"error": "Key not found", "key": key})
        else:
            self._send_json_response(400, {"error": "Invalid DELETE route"})

    def log_message(self, format, *args):
        # Silence default stderr request logging
        pass
