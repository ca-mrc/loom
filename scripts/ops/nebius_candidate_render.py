"""Render an already-resolved candidate with its exact source, without credentials."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from typing import Any

from scripts.ops.deploy_nebius_platform import load_render
from scripts.ops.nebius_certificate_gateway import run_private

from loom_service.environment_management.manager import CandidateBundle

Document = dict[str, Any]


def render_candidate_source(*, bundle: CandidateBundle, source_files: dict[str, bytes],
        config: Document, keyring: Document) -> dict[str, list[Document]]:
    """Run only the approved candidate's renderer/imports in a private directory.

    Inputs came from the installed publication reader, not runner file uploads.
    Do not inherit Python paths or operator credentials into the renderer. Its
    output remains subject to the existing deployment and shared-pool validators.
    """
    with tempfile.TemporaryDirectory(prefix='loom-participant-render-') as directory:
        root = Path(directory)
        for name, raw in source_files.items():
            path = root / name
            if (not path.is_relative_to(root) or '..' in path.parts or path.is_symlink()
                    or not (name.startswith(('deploy/k8s/', 'src/loom/'))
                        or name == 'scripts/ops/render_nebius_platform.py')):
                raise ValueError('candidate rendering source outside scope')
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
        for name, value in {'environment': config, 'candidate': bundle.candidate,
                'runtime-profile': bundle.profile, 'keyring': keyring}.items():
            (root / (name + '.json')).write_text(json.dumps(value, allow_nan=False))
        run_private([sys.executable, '-I', str(root / 'scripts/ops/render_nebius_platform.py'),
            '--environment-config', str(root / 'environment.json'), '--candidate', str(root / 'candidate.json'),
            '--runtime-profile', str(root / 'runtime-profile.json'), '--trusted-keyring', str(root / 'keyring.json'),
            '--output', str(root / 'render'), '--regional-output', str(root / 'regional')], timeout=60)
        return load_render(root / 'render')[2]
