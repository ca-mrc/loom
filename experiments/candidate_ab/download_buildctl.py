#!/usr/bin/env python3
"""Bound early buildctl admission only while native artifact download runs.

The real build command and candidate helper are unchanged. Three file-lock slots
are available after the native-download completion marker, irrespective of the
one/two/three-slot download-phase treatment. Intended only for this experiment.
"""
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import time


def append(path, record):
    with path.open('a') as output:
        fcntl.flock(output, fcntl.LOCK_EX)
        output.write(json.dumps(record) + '\n')
        output.flush()


def main():
    directory = Path(os.environ['AB_GATE_DIR'])
    directory.mkdir(parents=True, exist_ok=True)
    limit = int(os.environ['AB_DOWNLOAD_WORKERS'])
    if limit not in (1, 2, 3):
        raise ValueError('download workers must be 1, 2 or 3')
    done = directory / 'download-done.json'
    event = directory / 'gate-events.jsonl'
    started = time.time()
    while True:
        available = 3 if done.exists() else limit
        for slot in range(available):
            handle = (directory / f'slot-{slot}').open('a')
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                handle.close()
                continue
            component = next((x.rsplit('dest=', 1)[1].rsplit('/', 1)[-1] for x in sys.argv[1:] if 'dest=' in x), 'unknown')
            record = {'component': component, 'slot': slot, 'pid': os.getpid(), 'time': time.time(), 'monotonic': time.monotonic(),
                      'phase': 'after-download' if done.exists() else 'download', 'download_workers': limit, 'gate_wait_seconds': time.time() - started}
            append(event, record | {'event': 'start'})
            try:
                result = subprocess.run([os.environ['AB_REAL_BUILDCTL'], *sys.argv[1:]], check=False)
                returncode = result.returncode
            except BaseException:
                returncode = 1
                raise
            finally:
                append(event, record | {'event': 'end', 'time': time.time(), 'monotonic': time.monotonic(), 'returncode': returncode})
                handle.close()
            return returncode
        time.sleep(0.05)


if __name__ == '__main__':
    raise SystemExit(main())
