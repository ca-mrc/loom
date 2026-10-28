"""Bounded termination-message transport; image receipt authority stays with its reader."""
from __future__ import annotations

import base64
import json
import zlib
from typing import Any

PUBLICATION_MESSAGE_BYTES = 4000
# Eight archive descriptors: each has <=1024 UTF-8 key bytes (at most 6x
# JSON escaping), <=1024 printable version characters (at most 2x), a <=272
# image reference, <=136 component characters and bounded fixed fields.
# Add the existing 4 KiB cumulative metadata preview and eight registry refs.
# This fits below 128 KiB; decoding never allocates beyond that fixed bound.
MAX_DECODED_PUBLICATION_BYTES = 128 * 1024
_ENCODING = "loom.image-publication.zlib-base64.v1"


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate publication receipt field")
        result[key] = value
    return result


def encode_publication_receipt(receipt: dict[str, Any]) -> str:
    """Keep existing small JSON unchanged; compress only an oversized receipt."""
    body = json.dumps(receipt)
    size = len(body.encode("utf-8"))
    if size > MAX_DECODED_PUBLICATION_BYTES:
        raise ValueError("publication receipt exceeds decoded byte budget")
    if size <= PUBLICATION_MESSAGE_BYTES:
        return body
    message = json.dumps({
        "encoding": _ENCODING,
        "data": base64.b64encode(zlib.compress(body.encode("utf-8"))).decode("ascii"),
    })
    if len(message.encode("utf-8")) > PUBLICATION_MESSAGE_BYTES:
        raise ValueError("publication receipt exceeds Kubernetes termination message limit")
    return message


def decode_publication_receipt(message: str) -> dict[str, Any]:
    """Reject malformed, truncated or over-budget transport without a fallback."""
    try:
        if not isinstance(message, str) or len(message.encode("utf-8")) > PUBLICATION_MESSAGE_BYTES:
            raise ValueError("invalid publication receipt size")
        receipt = json.loads(message, object_pairs_hook=_object)
        if not isinstance(receipt, dict):
            raise ValueError("publication receipt must be an object")
        if "encoding" not in receipt:
            return receipt
        if set(receipt) != {"encoding", "data"} or receipt["encoding"] != _ENCODING or not isinstance(receipt["data"], str):
            raise ValueError("unsupported publication receipt encoding")
        compressed = base64.b64decode(receipt["data"], validate=True)
        decompressor = zlib.decompressobj()
        body = decompressor.decompress(compressed, MAX_DECODED_PUBLICATION_BYTES + 1)
        if (len(body) > MAX_DECODED_PUBLICATION_BYTES or not decompressor.eof
                or decompressor.unconsumed_tail or decompressor.unused_data):
            raise ValueError("invalid publication receipt compressed stream")
        receipt = json.loads(body.decode("utf-8"), object_pairs_hook=_object)
        if not isinstance(receipt, dict) or "encoding" in receipt:
            raise ValueError("invalid decoded publication receipt")
        return receipt
    except (ValueError, UnicodeError, zlib.error, RecursionError):
        raise ValueError("invalid publication receipt transport") from None
