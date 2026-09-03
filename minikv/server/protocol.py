"""Redis Serialization Protocol (RESP2 / RESP3) Parser and Serializer."""

from typing import Any, List, Union, Tuple, Optional

class RESPParser:
    @staticmethod
    def serialize_simple_string(s: str) -> bytes:
        return f"+{s}
".encode("utf-8")

    @staticmethod
    def serialize_error(err: str) -> bytes:
        return f"-ERR {err}
".encode("utf-8")

    @staticmethod
    def serialize_integer(num: int) -> bytes:
        return f":{num}
".encode("utf-8")

    @staticmethod
    def serialize_bulk_string(val: Optional[str]) -> bytes:
        if val is None:
            return b"$-1
"
        b = val.encode("utf-8")
        return f"${len(b)}
".encode("utf-8") + b + b"
"

    @staticmethod
    def serialize_array(items: List[Any]) -> bytes:
        out = [f"*{len(items)}
".encode("utf-8")]
        for item in items:
            if isinstance(item, str):
                out.append(RESPParser.serialize_bulk_string(item))
            elif isinstance(item, int):
                out.append(RESPParser.serialize_integer(item))
            elif item is None:
                out.append(b"$-1
")
            elif isinstance(item, list):
                out.append(RESPParser.serialize_array(item))
            elif isinstance(item, bytes):
                out.append(f"${len(item)}
".encode("utf-8") + item + b"
")
        return b"".join(out)

    @classmethod
    def parse_command(cls, raw: bytes) -> Tuple[Optional[List[str]], bytes]:
        if not raw:
            return None, raw
        if raw.startswith(b"*"):
            lines = raw.split(b"
")
            try:
                num_args = int(lines[0][1:])
                args = []
                idx = 1
                for _ in range(num_args):
                    if idx >= len(lines):
                        return None, raw
                    line = lines[idx]
                    if line.startswith(b"$"):
                        length = int(line[1:])
                        idx += 1
                        val = lines[idx][:length].decode("utf-8")
                        args.append(val)
                        idx += 1
                consumed = b"
".join(lines[:idx]) + b"
"
                return args, raw[len(consumed):]
            except Exception:
                return None, raw
        else:
            line, sep, rest = raw.partition(b"
")
            if not sep:
                return None, raw
            tokens = line.decode("utf-8").strip().split()
            return tokens, rest
