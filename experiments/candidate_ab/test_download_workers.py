"""Exercise actual lock admission, phase restoration and command failures."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

WRAPPER = Path(__file__).with_name('download_buildctl.py')


class AdmissionTests(unittest.TestCase):
    def check_limit(self, limit):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fake = root / 'buildctl'
            fake.write_text('#!/usr/bin/env python3\nimport time\ntime.sleep(1)\n')
            fake.chmod(0o700)
            env = os.environ | {'AB_GATE_DIR': str(root), 'AB_REAL_BUILDCTL': str(fake), 'AB_DOWNLOAD_WORKERS': str(limit)}
            children = [subprocess.Popen([sys.executable, str(WRAPPER), '--output', f'dest=/test/{i}.tar'], env=env) for i in range(3)]
            try:
                deadline = time.monotonic() + 10
                events = []
                while sum(x['event'] == 'start' for x in events) < limit:
                    if time.monotonic() > deadline:
                        self.fail('admission did not start')
                    time.sleep(.02)
                    events = [json.loads(x) for x in (root / 'gate-events.jsonl').read_text().splitlines()] if (root / 'gate-events.jsonl').exists() else []
                self.assertEqual(sum(x['event'] == 'start' for x in events), limit)
                self.assertFalse(any(x['event'] == 'end' for x in events))
                (root / 'download-done.json').write_text('{}')
                self.assertEqual([child.wait(timeout=10) for child in children], [0, 0, 0])
                events = [json.loads(x) for x in (root / 'gate-events.jsonl').read_text().splitlines()]
                starts = [x for x in events if x['event'] == 'start']
                self.assertEqual(len(starts), 3)
                self.assertEqual(sum(x['phase'] == 'download' for x in starts), limit)
                self.assertLess(max(x['time'] for x in starts), min(x['time'] for x in events if x['event'] == 'end'))
            finally:
                for child in children:
                    if child.poll() is None:
                        child.kill()
                        child.wait()

    def test_one(self): self.check_limit(1)
    def test_two(self): self.check_limit(2)
    def test_three(self): self.check_limit(3)

    def test_failure_propagates(self):
        with tempfile.TemporaryDirectory() as temporary:
            fake = Path(temporary) / 'buildctl'
            fake.write_text('#!/bin/sh\nexit 7\n')
            fake.chmod(0o700)
            env = os.environ | {'AB_GATE_DIR': temporary, 'AB_REAL_BUILDCTL': str(fake), 'AB_DOWNLOAD_WORKERS': '1'}
            result = subprocess.run([sys.executable, str(WRAPPER)], env=env)
            self.assertEqual(result.returncode, 7)


if __name__ == '__main__':
    unittest.main()
