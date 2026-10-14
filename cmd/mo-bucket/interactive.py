"""Verify native incremental freshness, stable sessions, DDL and catch-up.

Creates a unique child prefix. Requires the patched native binary and S3 API.
"""
import argparse
import json
import shutil
import statistics
import time
import uuid
from pathlib import Path
from runtime import BucketDatabase


def run(config, binary, output, repeats=25):
    cfg = dict(config, prefix=config['prefix'].strip('/') + '/interactive-' + str(uuid.uuid4()), lease_seconds=6)
    report = {'prefix': cfg['prefix'], 'checks': [], 'refresh_seconds': [], 'steady_seconds': []}
    w = BucketDatabase(cfg, binary, mode='writer', startup_timeout=180)
    r = BucketDatabase(cfg, binary, database='live', startup_timeout=180)
    def rows(db, sql):
        return db.execute(sql)['rows']
    def check(name):
        report['checks'].append(name)
        print('PASS', name, flush=True)
    try:
        w.attach()
        for sql in ['create database live', 'use live', 'create table numbers(id bigint primary key,value bigint)', 'insert into numbers values(1,0)']:
            w.execute(sql)
        r.attach()
        pid = r.runtime['process'].pid
        connection_id = rows(r, 'select connection_id()')
        report['reader_pid'] = pid
        report['connection_id'] = connection_id
        r.execute('set @kept=42')
        r.execute("prepare p from 'select sum(value) from numbers'")
        for i in range(repeats):
            w.execute('update numbers set value=value+1 where id=1')
            start = time.monotonic()
            assert rows(r, 'execute p') == [[str(i+1)]]
            report['refresh_seconds'].append(time.monotonic()-start)
            assert r.runtime['process'].pid == pid
            assert rows(r, 'select connection_id(),@kept') == [[connection_id[0][0], 42]]
            start = time.monotonic()
            assert rows(r, 'select value from numbers') == [[i+1]]
            report['steady_seconds'].append(time.monotonic()-start)
        check('frequent commits: fresh prepared SQL, same PID, connection and session variable')
        r.execute('begin')
        assert rows(r, 'select value from numbers') == [[repeats]]
        w.execute('update numbers set value=value+100 where id=1')
        assert rows(r, 'select value from numbers') == [[repeats]]
        r.execute('rollback')
        assert rows(r, 'execute p') == [[str(repeats+100)]]
        check('transaction remains pinned; next query sees new commit after rollback')
        for _ in range(15):
            w.execute('update numbers set value=value+1 where id=1')
        # Exercise source commits older than the private reader's local clock.
        time.sleep(2)
        start = time.monotonic()
        assert rows(r, 'execute p') == [[str(repeats+115)]]
        report['batch_catchup_seconds'] = time.monotonic()-start
        check('15 missed commits apply as one contiguous native batch')
        w.execute('create table added(id bigint primary key,name varchar(20))')
        w.execute("insert into added values(1,'new')")
        assert rows(r, 'select * from added') == [[1, 'new']]
        check('new table and rows become visible without reconnect')
        w.execute('alter table added add column extra bigint default 7')
        assert rows(r, 'select extra from added') == [[7]]
        check('schema change updates native catalog and query cache')
        w.execute('drop table added')
        w.execute('create table added(id bigint primary key,other bigint)')
        w.execute('insert into added values(2,9)')
        assert rows(r, 'select * from added') == [[2,9]]
        check('drop and recreate resolves the new native table identity')
        w.execute("select mo_ctl('dn','checkpoint','')")
        w.execute('update numbers set value=value+1 where id=1')
        assert rows(r, 'execute p') == [[str(repeats+116)]]
        check('incremental reader remains correct across native checkpoint')
        # Keep the reader and its prepared statement alive across writer loss.
        w.runtime['process'].kill()
        w.runtime['process'].wait()
        w.close()
        time.sleep(7)
        w = BucketDatabase(cfg, binary, mode='writer', database='live', startup_timeout=180)
        w.attach()
        w.execute('update numbers set value=value+1 where id=1')
        assert rows(r, 'execute p') == [[str(repeats+117)]]
        check('same live reader follows a recovered writer after SIGKILL and lease takeover')
        assert r.runtime['process'].pid == pid
        assert rows(r, 'select connection_id(),@kept') == [[connection_id[0][0],42]]
        for key in ['refresh_seconds','steady_seconds']:
            samples = sorted(report[key])
            report[key+'_median'] = statistics.median(samples)
            report[key+'_p95'] = samples[min(len(samples)-1, int(len(samples)*.95))]
        report['status'] = 'passed'
    except BaseException as err:
        report['status'] = 'failed'
        report['error'] = repr(err)
        for name, db in [('writer',w),('reader',r)]:
            if db.runtime:
                shutil.copy(Path(db.runtime['directory'].name)/'server.log',output.with_name(output.stem+'-'+name+'.log'))
        raise
    finally:
        output.write_text(json.dumps(report, indent=2))
        r.close()
        w.close()
    return report


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--config', type=Path, required=True)
    p.add_argument('--binary', required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--repeats', type=int, default=25)
    args = p.parse_args()
    print(json.dumps(run(json.loads(args.config.read_text()), args.binary, args.output, args.repeats), indent=2))
