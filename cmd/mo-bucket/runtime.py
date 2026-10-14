"""Local MO runtime backed only by a bucket, with automatic reader snapshots.

Dependencies: pymysql, boto3. The native binary is mo-service built with the
experimental --bucket-config integration. Internal MO roles run in that one
native process; this driver hides their launch configuration and cache lifetime.
"""
import argparse
import math
import decimal
import json
import os
from pathlib import Path
import random
import socket
import subprocess
import tempfile
import threading
import time
import urllib.request
import urllib.parse

import boto3
from botocore.config import Config as ClientConfig
import pymysql

LOG_UUID = '7c4dccb4-4d3c-41f8-b482-5251dc7a41bf'
CN_UUID = 'dd1dccb4-4d3c-41f8-b482-5251dc7a41bf'
TN_UUID = 'dd4dccb4-4d3c-41f8-b482-5251dc7a41bf'

def _ports():
    for _ in range(100):
        offset = random.randrange(100, 9000, 50)
        ports = [6001 + offset, 7001 + offset, 8001 + offset]
        ports += list(range(18000 + offset, 18020 + offset))
        ports += list(range(19000 + offset, 19020 + offset))
        ports += list(range(32000 + offset, 32010 + offset))
        reserved = []
        try:
            for port in ports:
                sock = socket.socket()
                sock.bind(('127.0.0.1', port))
                reserved.append(sock)
            return offset
        except OSError:
            pass
        finally:
            for sock in reserved:
                sock.close()
    raise RuntimeError('no free local MO port group')

def write_launch(directory, offset):
    directory = Path(directory)
    common = f'''data-dir = "{directory}/cache"
[log]
level = "error"
[[fileservice]]
name = "LOCAL"
backend = "BUCKET"
data-dir = "{directory}/cache/local"
[[fileservice]]
name = "SHARED"
backend = "BUCKET"
[[fileservice]]
name = "ETL"
backend = "DISK-ETL"
data-dir = "{directory}/cache/etl"
[observability]
disableMetric = true
disableTrace = true
status-port = {7001 + offset}
'''
    for role in ['cn', 'tn', 'log']:
        text = f'service-type = "{role.upper()}"\n' + common
        if role == 'cn':
            text += f'''[hakeeper-client]
service-addresses = ["127.0.0.1:{32001 + offset}"]
[cn]
uuid = "{CN_UUID}"
port-base = {18000 + offset}
sql-address = "127.0.0.1:{6001 + offset}"
[cn.frontend]
port = {6001 + offset}
[cn.cluster]
refresh-interval = "2s"
'''
        elif role == 'tn':
            text += f'''[hakeeper-client]
service-addresses = ["127.0.0.1:{32001 + offset}"]
[tn]
uuid = "{TN_UUID}"
port-base = {19000 + offset}
[tn.Txn.Storage]
backend = "TAE"
fileservice = "SHARED"
log-backend = "logservice"
[tn.GC]
disable-gc = true
[tn.cluster]
refresh-interval = "2s"
'''
        else:
            text += f'''[hakeeper-client]
service-addresses = ["127.0.0.1:{32001 + offset}"]
[logservice]
uuid = "{LOG_UUID}"
# One local replica: use a loopback RTT, retaining the native election protocol.
rttmillisecond = 50
logservice-port = {32001 + offset}
raft-port = {32000 + offset}
gossip-port = {32002 + offset}
gossip-seed-addresses = ["127.0.0.1:{32002 + offset}"]
'''
        (directory / f'{role}.toml').write_text(text)
    launch = '\n'.join(f'{key}=["{directory}/{role}.toml"]' for key, role in
                       [('logservices', 'log'), ('tnservices', 'tn'), ('cnservices', 'cn')])
    (directory / 'launch.toml').write_text(launch)

class BucketDatabase:
    def __init__(self, config, binary, *, mode='reader', database=None,
                 snapshot=None, user='root', password='111', startup_timeout=120,
                 freshness_ms=None, poll_interval_ms=50, background_sync=True,
                 idle_poll_interval_ms=1000):
        self.config = dict(config)
        self.binary = str(Path(binary).resolve())
        self.mode = mode
        self.database = database
        self.fixed_snapshot = snapshot
        self.user, self.password = user, password
        self.startup_timeout = startup_timeout
        self.lock = threading.RLock()
        self.runtime = None
        self.in_transaction = False
        self.session_settings = []
        self.freshness_ms = config.get('freshness_ms', 0) if freshness_ms is None else freshness_ms
        if not isinstance(self.freshness_ms, (int, float)) or not math.isfinite(self.freshness_ms) or self.freshness_ms < 0:
            raise ValueError('freshness_ms must be a finite nonnegative number')
        if not isinstance(poll_interval_ms, (int, float)) or not math.isfinite(poll_interval_ms) or poll_interval_ms <= 0:
            raise ValueError('poll_interval_ms must be a finite positive number')
        if not isinstance(idle_poll_interval_ms, (int, float)) or not math.isfinite(idle_poll_interval_ms) or idle_poll_interval_ms <= 0:
            raise ValueError('idle_poll_interval_ms must be a finite positive number')
        self.poll_interval = poll_interval_ms / 1000
        self.idle_poll_interval = max(self.poll_interval, idle_poll_interval_ms / 1000)
        self._last_activity = time.monotonic()
        self._watch_wake = None
        self.background_sync = background_sync
        self._state = threading.Condition()
        self._observation = None
        self._watcher = None
        self._watch_stop = None
        self._watch_runtime = None
        self._watch_error = None
        self._stats = {'foreground_head_checks': 0, 'background_head_checks': 0,
                       'prefetched_versions': 0, 'background_errors': 0}

        options = {'region_name': config.get('region', 'us-east-1'),
                   'config': ClientConfig(connect_timeout=2, read_timeout=5, retries={'total_max_attempts': 2})}
        if config.get('endpoint'):
            options['endpoint_url'] = config['endpoint']
        key = config.get('access_key') or os.environ.get('MO_BUCKET_ACCESS_KEY')
        if key:
            options['aws_access_key_id'] = key
            options['aws_secret_access_key'] = config.get('secret_key') or os.environ.get('MO_BUCKET_SECRET_KEY')
        self.s3 = boto3.client('s3', **options)

    def _head(self):
        obj = self.s3.get_object(Bucket=self.config['bucket'],
                                 Key=self.config['prefix'].strip('/') + '/head.json')
        try:
            return json.loads(obj['Body'].read())
        finally:
            obj['Body'].close()

    def _start(self, version):
        directory = tempfile.TemporaryDirectory(prefix='mo-bucket-runtime-')
        root = Path(directory.name)
        offset = _ports()
        write_launch(root, offset)
        cfg = dict(self.config, mode=self.mode)
        cfg.setdefault("live_updates", True)
        if version:
            cfg['snapshot'] = version
        # Native credentials use the injected/default chain or environment;
        # avoid writing credentials to generated runtime config files.
        env = dict(os.environ)
        if cfg.get('access_key'):
            env['MO_BUCKET_ACCESS_KEY'] = cfg.pop('access_key')
            env['MO_BUCKET_SECRET_KEY'] = cfg.pop('secret_key')
        (root / 'bucket.json').write_text(json.dumps(cfg))
        log = (root / 'server.log').open('w')
        child = subprocess.Popen([self.binary, '--bucket-config', str(root / 'bucket.json'),
                                  '--debug-http', f'127.0.0.1:{8001 + offset}',
                                  '--launch', str(root / 'launch.toml')], cwd=root, env=env,
                                 stdout=log, stderr=log)
        runtime = {'directory': directory, 'log': log, 'process': child,
                   'version': version, 'connection': None, 'port': 6001 + offset,
                   'control_port': 8001 + offset, 'live': cfg['live_updates'],
                   'applied_lsn': 0, 'checked_at': None}
        deadline = time.monotonic() + self.startup_timeout
        try:
            while time.monotonic() < deadline:
                if child.poll() is not None:
                    raise RuntimeError((root / 'server.log').read_text()[-10000:])
                connection = None
                try:
                    connection = pymysql.connect(host='127.0.0.1', port=6001 + offset,
                                                 user=self.user, password=self.password,
                                                 autocommit=True, connect_timeout=1,
                                                 read_timeout=60)
                    with connection.cursor() as cursor:
                        cursor.execute('select 1')
                        cursor.fetchall()
                    runtime['connection'] = connection
                    if self.database:
                        connection.select_db(self.database)
                    for setting in self.session_settings:
                        with connection.cursor() as cursor:
                            cursor.execute(setting)
                    return runtime
                except (pymysql.OperationalError, OSError):
                    if connection is not None:
                        connection.close()
                        runtime['connection'] = None
                    time.sleep(.1)
            raise TimeoutError((root / 'server.log').read_text()[-10000:])
        except BaseException:
            self._stop(runtime)
            raise

    @staticmethod
    def _stop(runtime):
        if runtime is None:
            return
        if runtime['connection']:
            runtime['connection'].close()
        child = runtime['process']
        if child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=30)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
        runtime['log'].close()
        runtime['directory'].cleanup()

    @staticmethod
    def _version(head):
        return head.get('visible_version') or head['version']

    def _observe(self, background=False):
        started = time.monotonic()
        with self._state:
            key = 'background_head_checks' if background else 'foreground_head_checks'
            self._stats[key] += 1
        head = self._head()
        return {'version': self._version(head), 'checked_at': started}

    @staticmethod
    def _control(runtime, action, version, timeout=60):
        url = f"http://127.0.0.1:{runtime['control_port']}/_bucket/{action}?" + urllib.parse.urlencode({'version': version})
        with urllib.request.urlopen(urllib.request.Request(url, method='POST'), timeout=timeout) as response:
            result = json.load(response)
        if result['version'] != version:
            raise RuntimeError('source version mismatch')
        return result

    def _publish_observation(self, runtime, observation):
        with self._state:
            if self.runtime is not runtime:
                return
            old = self._observation
            # Do not let a late background response regress a newer applied
            # source frontier or replace a newer successful observation.
            if observation.get('lsn', 0) < runtime['applied_lsn']:
                return
            if old is None or observation['checked_at'] >= old['checked_at']:
                self._observation = dict(observation)
                self._watch_error = None
                self._state.notify_all()

    def _mark_activity(self):
        with self._state:
            now = time.monotonic()
            if now - self._last_activity > 2 and self._watch_wake is not None:
                self._watch_wake.set()
            self._last_activity = now

    def _watch(self, runtime, stop, wake):
        while not stop.is_set():
            try:
                observation = self._observe(background=True)
                if stop.is_set():
                    return
                with self._state:
                    applied_version, applied_lsn = runtime['version'], runtime['applied_lsn']
                    prepared = dict(self._observation) if self._observation else None
                if prepared and prepared['version'] == observation['version'] and prepared.get('lsn', 0) >= applied_lsn:
                    observation['lsn'] = prepared['lsn']
                elif observation['version'] != applied_version:
                    result = self._control(runtime, 'prefetch', observation['version'], timeout=5)
                    observation['lsn'] = result['lsn']
                    with self._state:
                        self._stats['prefetched_versions'] += 1
                else:
                    observation['lsn'] = applied_lsn
                if stop.is_set():
                    return
                self._publish_observation(runtime, observation)
            except Exception as err:
                with self._state:
                    if self.runtime is not runtime:
                        return
                    self._stats['background_errors'] += 1
                    self._watch_error = str(err)
                    self._state.notify_all()
            # Wait is cancellable and never holds the SQL connection lock.
            with self._state:
                delay = self.idle_poll_interval if time.monotonic()-self._last_activity > 2 else self.poll_interval
            wake.wait(delay)
            wake.clear()

    def attach(self):
        with self.lock:
            if self.runtime is None:
                observation = None
                version = None
                if self.mode == 'reader':
                    observation = self._observe()
                    version = self.fixed_snapshot or observation['version']
                    if not version:
                        raise RuntimeError('no committed database version yet')
                runtime = self._start(version)
                self.runtime = runtime
                with self._state:
                    self._observation = None
                    self._watch_error = None
                    if observation and not self.fixed_snapshot:
                        runtime['checked_at'] = observation['checked_at']
                    if self.mode == 'reader' and runtime['live'] and not self.fixed_snapshot and self.background_sync:
                        stop = threading.Event()
                        wake = threading.Event()
                        self._watch_wake = wake
                        self._last_activity = time.monotonic()
                        self._watch_stop = stop
                        self._watch_runtime = runtime
                        self._watcher = threading.Thread(target=self._watch, args=(runtime, stop, wake),
                                                        name=f"mo-bucket-prefetch-{runtime['process'].pid}", daemon=True)
                        self._watcher.start()
        return self

    def _refresh(self, force=False, retry_expired=True):
        runtime = self.runtime
        observation = None
        if self.freshness_ms and not force:
            with self._state:
                candidate = self._observation
                if candidate and candidate.get('lsn', 0) >= runtime['applied_lsn'] and (time.monotonic()-candidate['checked_at'])*1000 <= self.freshness_ms:
                    observation = dict(candidate)
        if observation is None:
            observation = self._observe()
        version = observation['version']
        if version != runtime['version']:
            if runtime['live']:
                synced = self._control(runtime, 'sync', version)
                observation['lsn'] = synced['lsn']
                with self._state:
                    runtime['version'] = version
                    runtime['applied_lsn'] = synced['lsn']
                    if self._observation and self._observation.get('lsn', 0) < synced['lsn']:
                        self._observation = None
            else:
                replacement = self._start(version)
                old, self.runtime = runtime, replacement
                self._stop(old)
                runtime = replacement
        else:
            observation['lsn'] = runtime['applied_lsn']
        # A slow/cancelled fetch cannot extend the freshness window simply by
        # completing late. The age starts when its remote HEAD request began.
        if self.freshness_ms and not force and (time.monotonic()-observation['checked_at'])*1000 > self.freshness_ms:
            if retry_expired:
                return self._refresh(retry_expired=False)
            raise RuntimeError('freshness window expired during synchronization; retry or increase freshness_ms')
        runtime['checked_at'] = observation['checked_at']
        self._publish_observation(runtime, observation)

    def sync(self):
        """Read remote HEAD and catch up now; never advance an open transaction."""
        with self.lock:
            self.attach()
            self._mark_activity()
            if self.mode != 'reader' or self.fixed_snapshot:
                raise RuntimeError('sync requires a live reader')
            if self.in_transaction:
                raise RuntimeError('end the current transaction before syncing')
            self._refresh(force=True)
            return {'snapshot': self.runtime['version'], 'lsn': self.runtime['applied_lsn']}

    def status(self):
        """Inspect freshness policy and background health without remote I/O."""
        with self._state:
            observation = dict(self._observation) if self._observation else None
            return {'freshness_ms': self.freshness_ms, 'observation': observation,
                    'background_error': self._watch_error, 'stats': dict(self._stats),
                    'watcher_alive': bool(self._watcher and self._watcher.is_alive())}

    def execute(self, sql, params=None):
        with self.lock:
            self.attach()
            self._mark_activity()
            if self.mode == 'reader' and not self.in_transaction and not self.fixed_snapshot:
                self._refresh()
            checked_at = self.runtime['checked_at']
            freshness = {'mode': 'snapshot' if self.fixed_snapshot else ('bounded' if self.freshness_ms else 'latest'),
                         'max_staleness_ms': self.freshness_ms,
                         'checked_age_ms': (time.monotonic()-checked_at)*1000 if checked_at is not None else None,
                         'pinned': self.in_transaction or bool(self.fixed_snapshot)}
            if self.mode == 'reader' and self.freshness_ms and not freshness['pinned'] and freshness['checked_age_ms'] > self.freshness_ms:
                raise RuntimeError('freshness window expired before query admission; retry')
            connection = self.runtime['connection']
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                columns = [col[0] for col in cursor.description] if cursor.description else []
                rows = [[str(v) if isinstance(v, decimal.Decimal) else v for v in row]
                        for row in cursor.fetchall()] if cursor.description else []
                affected = cursor.rowcount
            self.in_transaction = bool(connection.server_status & 1)
            command = sql.strip().lower()
            if command.startswith('set '):
                self.session_settings.append(sql)
            if command.startswith('use '):
                with connection.cursor() as cursor:
                    cursor.execute('select database()')
                    self.database = cursor.fetchone()[0]
            version = self.runtime['version'] if self.mode == 'reader' else self._head().get('visible_version')
            freshness['pinned'] = freshness['pinned'] or self.in_transaction
            return {'columns': columns, 'rows': rows, 'affected_rows': affected, 'snapshot': version,
                    'freshness': freshness if self.mode == 'reader' else None}

    def close(self):
        with self.lock:
            old, self.runtime = self.runtime, None
            with self._state:
                watcher, stop, wake = self._watcher, self._watch_stop, self._watch_wake
                self._watcher = self._watch_stop = self._watch_runtime = self._watch_wake = None
                self._observation = None
            if stop is not None:
                stop.set()
            if wake is not None:
                wake.set()
            if watcher is not None:
                watcher.join(timeout=12)
            self._stop(old)

    def __enter__(self):
        return self.attach()

    def __exit__(self, *_):
        self.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--mode', choices=['writer', 'reader'], default='reader')
    parser.add_argument('--database')
    parser.add_argument('--snapshot')
    parser.add_argument('--freshness-ms', type=float, default=None)
    parser.add_argument('--no-background', action='store_true')
    parser.add_argument('--sql', action='append', required=True)
    args = parser.parse_args()
    cfg = json.loads(args.config.read_text())
    with BucketDatabase(cfg, args.binary, mode=args.mode, database=args.database,
                        snapshot=args.snapshot, freshness_ms=args.freshness_ms,
                        background_sync=not args.no_background) as db:
        for sql in args.sql:
            print(json.dumps(db.execute(sql), ensure_ascii=False), flush=True)

if __name__ == '__main__':
    main()
