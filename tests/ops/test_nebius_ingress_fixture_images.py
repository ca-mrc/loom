"""Disposable image transport keeps original pins when the public mirror is full."""
from __future__ import annotations

import pytest
from tests.cluster import test_nebius_ingress_operation as fixture


@pytest.mark.parametrize("image", [fixture.TRAEFIK, fixture.GUARD_PYTHON])
@pytest.mark.parametrize("failure", [None, "429 Too Many Requests: Data limit exceeded"])
def test_fixture_pull_preserves_manifest_and_original_name(monkeypatch, image, failure):
    mirror = image.replace("docker.io/library/", "public.ecr.aws/docker/library/")
    upstream = mirror.replace("public.ecr.aws/docker/library/", "docker.io/library/")
    digest = image.split("@", 1)[1]
    pulls, tags = [], []

    def run(container, *args, **kwargs):
        if args[:3] == ("ctr", "images", "pull"):
            assert args[3:6] == ("--platform", "linux/amd64", "--skip-metadata")
            assert kwargs["timeout"] == 180
            pulls.append(args[-1])
            if failure and args[-1] == mirror:
                raise AssertionError(failure)
            return ""
        if args == ("ctr", "images", "ls"):
            return f"{pulls[-1]} manifest {digest}"
        assert args[:3] == ("ctr", "images", "tag")
        tags.append(args[3:])
        return ""

    monkeypatch.setattr(fixture, "_run", run)
    fixture._pull_fixture_image(object(), image)
    source = upstream if failure else mirror
    assert pulls == ([mirror, upstream] if failure else [mirror])
    assert tags == ([(source, image)] if source != image else [])


@pytest.mark.parametrize("failure", ["401 Unauthorized", "manifest digest mismatch", "connection refused"])
def test_fixture_pull_does_not_hide_other_failures(monkeypatch, failure):
    pulls = []

    def run(container, *args, **kwargs):
        pulls.append(args)
        raise AssertionError(failure)

    monkeypatch.setattr(fixture, "_run", run)
    with pytest.raises(AssertionError, match=failure):
        fixture._pull_fixture_image(object(), fixture.TRAEFIK)
    assert len(pulls) == 1


@pytest.mark.parametrize("upstream_failure", [True, False])
def test_fixture_pull_never_aliases_failed_or_wrong_digest_fallback(monkeypatch, upstream_failure):
    pulls = []

    def run(container, *args, **kwargs):
        if args[:3] == ("ctr", "images", "pull"):
            pulls.append(args[-1])
            if len(pulls) == 1 or upstream_failure:
                raise AssertionError("429 Too Many Requests")
            return ""
        assert args == ("ctr", "images", "ls"), "must not alias unqualified content"
        return f"{pulls[-1]} manifest sha256:{'0' * 64}"

    monkeypatch.setattr(fixture, "_run", run)
    with pytest.raises(AssertionError):
        fixture._pull_fixture_image(object(), fixture.TRAEFIK)
    assert len(pulls) == 2
