"""Resolve the final OCI filesystem identity without extracting or executing it."""

import gzip
import hashlib
import io
import json
import tarfile

import pytest
import zstandard

from loom_execution_actuator.task_image_metadata import read_task_image_runtime_metadata


def _layer(entries):
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:") as archive:
        for name, value in entries.items():
            info = tarfile.TarInfo(name)
            if isinstance(value, tuple):
                info.type, info.linkname = value
                archive.addfile(info)
            else:
                info.size = len(value)
                archive.addfile(info, io.BytesIO(value))
    return output.getvalue()


def _image(path, layers, config=None, *, directory=False, compression="gzip"):
    files = {"oci-layout": b'{"imageLayoutVersion":"1.0.0"}'}
    def blob(data, media):
        digest = hashlib.sha256(data).hexdigest()
        files["blobs/sha256/" + digest] = data
        return {"mediaType": media, "digest": "sha256:" + digest, "size": len(data)}
    media = "application/vnd.oci.image.layer.v1.tar"
    descriptors = []
    for layer in layers:
        body = gzip.compress(layer) if compression == "gzip" else (
            zstandard.ZstdCompressor().compress(layer) if compression == "zstd" else layer)
        descriptors.append(blob(body, media + ("+" + compression if compression else "")))
    document = {"os": "linux", "architecture": "amd64", "config": config or {},
                "rootfs": {"type": "layers", "diff_ids": ["sha256:" + hashlib.sha256(x).hexdigest() for x in layers]}}
    cfg = blob(json.dumps(document).encode(), "application/vnd.oci.image.config.v1+json")
    manifest = blob(json.dumps({"schemaVersion": 2, "config": cfg, "layers": descriptors}).encode(),
                    "application/vnd.oci.image.manifest.v1+json")
    files["index.json"] = json.dumps({"schemaVersion": 2, "manifests": [manifest]}).encode()
    if directory:
        path.mkdir()
        for name, body in files.items():
            target = path / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(body)
    else:
        with tarfile.open(path, "w:") as archive:
            for name, body in files.items():
                entry = tarfile.TarInfo(name)
                entry.size = len(body)
                archive.addfile(entry, io.BytesIO(body))
    return path


PASSWD = b"root:x:0:0:root:/root:/bin/sh\nworker:x:1001:1002::/home/worker:/bin/sh\n"
GROUP = b"root:x:0:\nworkers:x:1002:\nother:x:1003:\n"


@pytest.mark.parametrize("workdir", ["//tests", "/tests\x00hidden", "/" + "x" * 4096],
                         ids=["double-leading-slash", "nul-byte", "overlong"])
def test_rejects_unsafe_oci_workdir_before_runtime_execution(tmp_path, workdir):
    image = _image(tmp_path / "image", [], {"WorkingDir": workdir, "Env": ["HOME=/root"]})
    with pytest.raises(ValueError, match="OCI runtime metadata unsupported"):
        read_task_image_runtime_metadata(image)


@pytest.mark.parametrize("directory,compression,user,uid,gid", [
    (False, "gzip", "", 0, 0),
    (False, "gzip", "root", 0, 0),
    (False, "gzip", "worker", 1001, 1002),
    (False, "gzip", "1001", 1001, 1002),
    (False, "gzip", "1001:1003", 1001, 1003),
    (False, "gzip", "worker:other", 1001, 1003),
    (False, "", "worker:other", 1001, 1003),
    (False, "zstd", "worker:other", 1001, 1003),
    (True, "", "worker:other", 1001, 1003),
    (True, "gzip", "worker:other", 1001, 1003),
    (True, "zstd", "worker:other", 1001, 1003),
])
def test_final_config_identity_and_entrypoint(tmp_path, directory, compression, user, uid, gid):
    image = _image(tmp_path / "image", [_layer({"etc/passwd": PASSWD, "etc/group": GROUP})],
                   {"User": user, "WorkingDir": "/app", "Entrypoint": ["/entrypoint.sh"],
                    "Env": ["HOME=/home/explicit", "PATH=/bin:/usr/bin"]},
                   directory=directory, compression=compression)
    value = read_task_image_runtime_metadata(image, directory=directory)
    assert value == {"user": user, "uid": uid, "gid": gid, "home": "/home/explicit", "workdir": "/app",
                     "entrypoint": ["/entrypoint.sh"], "environment": {"HOME": "/home/explicit", "PATH": "/bin:/usr/bin"}}
    assert not (tmp_path / "etc").exists()


def test_supplementary_groups_follow_the_resolved_account(tmp_path):
    group = (
        b"root:x:0:\nworkers:x:1002:worker\nextra:x:1004:worker,other\n"
        b"later:x:1005:worker\n"
    )
    image = _image(tmp_path / "image", [_layer({"etc/passwd": PASSWD, "etc/group": group})], {"User": "worker"})
    value = read_task_image_runtime_metadata(image)
    assert value["supplementary_groups"] == [1004, 1005]
    numeric = _image(tmp_path / "numeric", [_layer({"etc/passwd": PASSWD, "etc/group": group})], {"User": "1001"})
    assert read_task_image_runtime_metadata(numeric)["supplementary_groups"] == [1004, 1005]


def test_last_layer_passwd_replaces_previous_identity(tmp_path):
    image = _image(tmp_path / "image", [_layer({"etc/passwd": PASSWD}),
                   _layer({"etc/passwd": b"worker:x:2001:2002::/home/final:/bin/sh\n"})], {"User": "worker"})
    result = read_task_image_runtime_metadata(image)
    assert (result["uid"], result["gid"], result["home"], result["workdir"]) == (2001, 2002, "/home/final", "/")


@pytest.mark.parametrize("directory", [False, True])
def test_literal_backslashes_in_linux_layer_names_do_not_alias_identity(tmp_path, directory):
    # Fedora's systemd RPM installs these escaped unit names in retro-console-soc.
    # A backslash is a literal POSIX filename character, including in a name
    # resembling passwd: it must neither reject the image nor replace /etc/passwd.
    layer = _layer({
        "etc/passwd": PASSWD, "etc/group": GROUP,
        r"usr/lib/systemd/system/system-systemd\x2dcryptsetup.slice": b"[Unit]\n",
        r"usr/lib/systemd/system/system-systemd\x2dveritysetup.slice": b"[Unit]\n",
        r"etc\passwd": b"worker:x:2001:2002::/home/spoofed:/bin/sh\n",
    })
    image = _image(tmp_path / "image", [layer], {"User": "worker"}, directory=directory)
    result = read_task_image_runtime_metadata(image, directory=directory)
    assert (result["uid"], result["gid"], result["home"]) == (1001, 1002, "/home/worker")
    assert not (tmp_path / "etc").exists()


@pytest.mark.parametrize("name", ["/etc/passwd", "etc/./passwd", "etc/x/../passwd", "etc//passwd"])
def test_identity_path_aliases_remain_rejected(tmp_path, name):
    image = _image(tmp_path / "image", [_layer({name: PASSWD})], {"User": "worker"})
    with pytest.raises(ValueError, match=r"^OCI runtime metadata unsupported$"):
        read_task_image_runtime_metadata(image)


@pytest.mark.parametrize("whiteout", ["etc/.wh.passwd", "etc/.wh..wh..opq", ".wh.etc", ".wh..wh..opq"])
def test_whiteouts_remove_lower_identity_even_when_tar_order_is_reversed(tmp_path, whiteout):
    image = _image(tmp_path / "image", [_layer({"etc/passwd": PASSWD}), _layer({whiteout: b""})],
                   {"User": "worker"})
    with pytest.raises(ValueError, match=r"^OCI runtime metadata unsupported$"):
        read_task_image_runtime_metadata(image)


def test_whiteout_does_not_remove_replacement_in_same_layer(tmp_path):
    image = _image(tmp_path / "image", [_layer({"etc/passwd": PASSWD}),
                   _layer({"etc/passwd": b"worker:x:2001:2002::/home/final:/bin/sh\n", "etc/.wh.passwd": b""})],
                   {"User": "worker"})
    assert read_task_image_runtime_metadata(image)["uid"] == 2001


@pytest.mark.parametrize("name", ["etc", "etc/passwd", "etc/group"])
@pytest.mark.parametrize("kind", [tarfile.SYMTYPE, tarfile.LNKTYPE])
def test_identity_link_is_rejected_without_following_it(tmp_path, name, kind):
    image = _image(tmp_path / "image", [_layer({"etc/passwd": PASSWD, "etc/group": GROUP}),
                   _layer({name: (kind, "/private-secret")})], {"User": "worker"})
    with pytest.raises(ValueError, match=r"^OCI runtime metadata unsupported$") as caught:
        read_task_image_runtime_metadata(image)
    assert caught.value.__cause__ is None


@pytest.mark.parametrize("config", [{"User": "missing"}, {"User": "worker:missing"}, {"User": False},
    {"Entrypoint": ""}, {"Env": ""},
    {"User": "2147483648:0", "Env": ["HOME=/home/x"]}, {"Env": ["HOME=/root", "HOME=/home/ambiguous"]}, {"WorkingDir": "/app/../private"},
    {"Entrypoint": "private-command"}, {"Entrypoint": ["x" * 20000]}])
def test_unsupported_metadata_errors_never_expose_config(tmp_path, config):
    image = _image(tmp_path / "image", [_layer({"etc/passwd": PASSWD, "etc/group": GROUP})], config)
    with pytest.raises(ValueError, match=r"^OCI runtime metadata unsupported$") as caught:
        read_task_image_runtime_metadata(image)
    assert caught.value.__cause__ is None


def test_numeric_user_without_passwd_needs_explicit_home(tmp_path):
    image = _image(tmp_path / "image", [_layer({})], {"User": "1234", "Env": ["HOME=/home/custom"]})
    value = read_task_image_runtime_metadata(image)
    assert (value["uid"], value["gid"], value["home"]) == (1234, 0, "/home/custom")


def test_expanded_layers_share_a_bounded_stream_budget(tmp_path, monkeypatch):
    import loom_execution_actuator.task_image_metadata as metadata

    monkeypatch.setattr(metadata, "_MAX_LAYER_BYTES", 1024)
    image = _image(tmp_path / "image", [_layer({"etc/passwd": PASSWD})])
    with pytest.raises(ValueError, match=r"^OCI runtime metadata unsupported$"):
        read_task_image_runtime_metadata(image)


def test_root_directory_tar_record_and_public_gpg_fingerprint_are_supported(tmp_path):
    layer = _layer({".": (tarfile.DIRTYPE, ""), "etc/passwd": PASSWD})
    image = _image(tmp_path / "image", [layer], {"Env": ["GPG_KEY=" + "A" * 40]})
    assert read_task_image_runtime_metadata(image)["environment"]["GPG_KEY"] == "A" * 40


@pytest.mark.parametrize("entry", ["API_KEY=private-value", "GPG_KEY=private-value", "VNC_KEY_DELAY_MS=private-value",
                                   "PASSWORD=diagnostic-fixture-value"])
def test_authored_fixture_environment_is_kept_for_the_task(tmp_path, entry):
    name, value = entry.split("=", 1)
    image = _image(tmp_path / "image", [_layer({"etc/passwd": PASSWD})], {"Env": [entry]})
    result = read_task_image_runtime_metadata(image)
    assert result["environment"][name] == value


@pytest.mark.parametrize("entry", ["KUBECONFIG=diagnostic-fixture-value", "LOOM_CONTROL=diagnostic-fixture-value",
                                   "HTTP_PROXY=diagnostic-fixture-value"])
def test_reserved_image_environment_names_the_variable_without_its_value(tmp_path, entry):
    name, value = entry.split("=", 1)
    image = _image(tmp_path / "image", [_layer({"etc/passwd": PASSWD})], {"Env": [entry]})
    with pytest.raises(ValueError, match=rf"^task image environment rejected {name}$") as caught:
        read_task_image_runtime_metadata(image)
    assert value not in str(caught.value)
    assert caught.value.__cause__ is None


def test_public_vnc_keyboard_delay_is_preserved(tmp_path):
    image = _image(tmp_path / "image", [_layer({"etc/passwd": PASSWD})], {"Env": ["VNC_KEY_DELAY_MS=35"]})
    assert read_task_image_runtime_metadata(image)["environment"]["VNC_KEY_DELAY_MS"] == "35"


def test_controller_contract_still_rejects_password_and_the_diagnostic_omits_the_value():
    import os

    from loom.runtime_environment import validate_runtime_environment
    from loom_execution_actuator.task_image_controller import _platform_diagnostic
    from loom_execution_actuator.task_image_runtime import _metadata_publication_error

    value = "diagnostic-fixture-value"
    assert "PASSWORD" not in os.environ
    with pytest.raises(ValueError, match="forbidden name"):
        validate_runtime_environment({"PASSWORD": value})
    message = _metadata_publication_error(ValueError("task image environment rejected KUBECONFIG"))
    diagnostic = _platform_diagnostic({"error": message}, exit_code=1)
    assert diagnostic == {"summary": "task image environment rejected KUBECONFIG", "exit_code": 1}
    assert value not in diagnostic["summary"]
    hidden = _metadata_publication_error(ValueError(f"USER={value}"))
    assert hidden == "OCI runtime metadata is unsupported"
    assert value not in hidden
