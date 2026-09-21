"""Safety boundaries for the standalone staged daemon updater."""
from contextlib import closing
import fcntl
import json
from pathlib import Path
import runpy
import sqlite3
import tempfile
import unittest
from unittest.mock import patch


UPDATER = Path(__file__).resolve().parents[1] / 'contrib/mlq-update'


class IdleUpdateTests(unittest.TestCase):
    def setUp(self):
        self.module = runpy.run_path(str(UPDATER))
        self.globals = self.module['activate'].__globals__
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / 'mlqueue.db'
        with closing(sqlite3.connect(self.db)) as db, db:
            db.executescript('CREATE TABLE jobs (finished_at INTEGER); '
                             'CREATE TABLE run_leases (released_at INTEGER);')
        self.target = self.root / 'installed'
        self.target.write_bytes(b'old daemon')
        staged = self.root / 'staged'
        staged.mkdir()
        self.candidate = staged / 'mlqd'
        self.candidate.write_bytes(b'new daemon')
        self.manifest = dict(candidate=str(self.candidate), target=str(self.target),
                             sha256=self.module['digest'](self.candidate), service='test.service')
        (self.root / 'pending.json').write_text(json.dumps(self.manifest))
        self.stopped = False

    def systemctl(self, *args):
        if args[0] == 'show':
            return {'KillMode': 'process', 'Transient': 'no', 'MainPID': '42',
                    'ExecStart': f'{{ path={self.target} ; }}'}[args[3]]
        self.stopped = args[0] == 'stop'
        return ''

    def activate(self):
        with patch.dict(self.globals, systemctl=self.systemctl,
                        status=lambda _: dict(pid=42, dbPath=str(self.db)), ready=lambda _: None):
            self.module['activate'](self.root)

    def test_terminal_history_does_not_block_idle_but_unknown_work_does(self):
        snapshot = dict(activeLeases=0, admissionBlocked=False, jobs=[dict(finishedAt=123)])
        self.assertTrue(self.module['idle_snapshot'](snapshot))
        snapshot['jobs'].append(dict(state='new_future_state'))
        self.assertFalse(self.module['idle_snapshot'](snapshot))
        snapshot['jobs'].pop()
        snapshot['activeLeases'] = 1
        self.assertFalse(self.module['idle_snapshot'](snapshot))

    def test_job_arriving_after_idle_snapshot_preserves_binary_and_pending_update(self):
        with closing(sqlite3.connect(self.db)) as db, db:
            db.execute('INSERT INTO jobs VALUES (NULL)')
        self.activate()
        self.assertEqual(self.target.read_bytes(), b'old daemon')
        self.assertTrue((self.root / 'pending.json').exists())
        self.assertFalse(self.stopped)

    def test_unreleased_lease_blocks_update_even_without_unfinished_job(self):
        with closing(sqlite3.connect(self.db)) as db, db:
            db.execute('INSERT INTO run_leases VALUES (NULL)')
        self.activate()
        self.assertEqual(self.target.read_bytes(), b'old daemon')
        self.assertTrue((self.root / 'pending.json').exists())
        self.assertFalse(self.stopped)

    def test_competing_daemon_lock_prevents_replacement(self):
        with (self.root / 'daemon.lock').open('w') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError):
                self.activate()
        self.assertEqual(self.target.read_bytes(), b'old daemon')
        self.assertTrue((self.root / 'pending.json').exists())
        self.assertFalse(self.stopped)

    def test_idle_activation_replaces_binary_and_preserves_previous_version(self):
        self.activate()
        self.assertEqual(self.target.read_bytes(), b'new daemon')
        self.assertEqual((self.root / 'previous-mlqd').read_bytes(), b'old daemon')
        self.assertFalse((self.root / 'pending.json').exists())
        self.assertFalse(self.stopped)


if __name__ == '__main__':
    unittest.main()
