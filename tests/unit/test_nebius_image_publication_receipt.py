"""Termination transport stays bounded and cannot turn malformed bytes into a receipt."""
import base64
import json
import random
import zlib

import pytest

from loom_execution_actuator.image_publication_receipt import (
    MAX_DECODED_PUBLICATION_BYTES,
    PUBLICATION_MESSAGE_BYTES,
    decode_publication_receipt,
    encode_publication_receipt,
)

ENCODING = "loom.image-publication.zlib-base64.v1"


def envelope(body):
    return json.dumps({"encoding": ENCODING, "data": base64.b64encode(body).decode()})


def test_small_existing_json_and_failure_messages_are_unchanged():
    for receipt in ({"materialization_id": "same", "lease_epoch": 3, "registry_images": {}},
                    {"phase": "publish", "error": "BuildPreparationError"}):
        message = json.dumps(receipt)
        assert encode_publication_receipt(receipt) == message
        assert decode_publication_receipt(message) == receipt


@pytest.mark.parametrize("size", [4000, MAX_DECODED_PUBLICATION_BYTES])
def test_exact_plain_and_decoded_byte_boundaries_are_supported(size):
    overhead = len(json.dumps({"value": ""}).encode())
    receipt = {"value": "x" * (size - overhead)}
    message = encode_publication_receipt(receipt)
    assert len(message.encode()) <= PUBLICATION_MESSAGE_BYTES
    assert decode_publication_receipt(message) == receipt


def test_encoder_rejects_decode_budget_and_incompressible_wire_overflow():
    with pytest.raises(ValueError, match="decoded byte budget"):
        encode_publication_receipt({"value": "x" * MAX_DECODED_PUBLICATION_BYTES})
    random_value = base64.b64encode(random.Random(0).randbytes(8000)).decode()
    with pytest.raises(ValueError, match="termination message limit"):
        encode_publication_receipt({"value": random_value})


def test_eight_schema_maximum_archive_descriptors_fit_the_decoded_budget():
    from loom.task_image_archives import MAX_ARCHIVE_BYTES, PreparedImageArchiveV1

    image = "registry.example/" + "r" * 183 + "@sha256:" + "a" * 64
    names = [f"component-{index}-".ljust(136, "x") for index in range(8)]
    archives = {name: PreparedImageArchiveV1.model_validate({
        "image_ref": image, "object": {
            "bucket": "b" * 63, "object_key": "\u0080" * 512,
            "version_id": '"' * 1024, "content_sha256": format(index, "064x"),
            "size_bytes": MAX_ARCHIVE_BYTES,
        },
    }).model_dump(mode="json") for index,name in enumerate(names)}
    receipt = {"materialization_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "lease_epoch": 2**63 - 1,
        "registry_images": dict.fromkeys(names, image), "image_archives": archives}
    assert 4000 < len(json.dumps(receipt).encode()) < MAX_DECODED_PUBLICATION_BYTES
    message = encode_publication_receipt(receipt)
    assert len(message.encode()) <= PUBLICATION_MESSAGE_BYTES
    assert decode_publication_receipt(message) == receipt


@pytest.mark.parametrize("damage", [
    "invalid_base64", "invalid_zlib", "truncated", "trailing",
    "concatenated", "zip_bomb", "invalid_utf8", "invalid_json", "duplicate_json",
    "not_object", "nested_envelope", "extra_envelope_key", "unknown_encoding", "data_not_string",
])
def test_compressed_receipt_rejects_malformed_or_unbounded_transport(damage):
    body = zlib.compress(b'{"registry_images":{}}')
    if damage == "invalid_zlib":
        body = b"not a zlib stream"
    elif damage == "truncated":
        body = body[:-1]
    elif damage == "trailing":
        body += b"trailing"
    elif damage == "concatenated":
        body += zlib.compress(b"{}")
    elif damage == "zip_bomb":
        body = zlib.compress(json.dumps({"value": "x" * MAX_DECODED_PUBLICATION_BYTES}).encode())
    elif damage == "invalid_utf8":
        body = zlib.compress(b'"\xff"')
    elif damage == "invalid_json":
        body = zlib.compress(b'{"unfinished":')
    elif damage == "duplicate_json":
        body = zlib.compress(b'{"identity":1,"identity":2}')
    elif damage == "not_object":
        body = zlib.compress(b"[]")
    elif damage == "nested_envelope":
        body = zlib.compress(envelope(body).encode())
    document = json.loads(envelope(body))
    if damage == "invalid_base64":
        document["data"] = "!!!!"
    elif damage == "extra_envelope_key":
        document["extra"] = True
    elif damage == "unknown_encoding":
        document["encoding"] = "future"
    elif damage == "data_not_string":
        document["data"] = 3
    with pytest.raises(ValueError, match="invalid publication receipt transport"):
        decode_publication_receipt(json.dumps(document))


@pytest.mark.parametrize("message", [None, "x" * 4001, "[]", '{"a":1,"a":2}', '{"bad":'])
def test_plain_receipt_rejects_malformed_or_over_budget_messages(message):
    with pytest.raises(ValueError, match="invalid publication receipt transport"):
        decode_publication_receipt(message)
