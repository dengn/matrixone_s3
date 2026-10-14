"""End-to-end native MO test. Requires an S3 bucket and the patched binary.

Run: python integration.py --config bucket.json --binary ./mo-service
Uses a new prefix; never deletes or changes an existing database.
"""
import argparse
import json
import shutil
import time
import uuid
from pathlib import Path
from runtime import BucketDatabase


def run(config, binary, evidence):
    cfg = dict(config, prefix=config['prefix'].strip('/') + '/integration-' + str(uuid.uuid4()))
    cfg['lease_seconds'] = 6
    report = {'prefix': cfg['prefix'], 'checks': [], 'timings_seconds': {}}
    instances = []
    def save():
        evidence.write_text(json.dumps(report, ensure_ascii=False, indent=2))
    def passed(name, detail=None):
        report['checks'].append({'name': name, 'detail': detail})
        save()
        print('PASS ' + name, flush=True)
    def database(mode, **kwargs):
        db = BucketDatabase(cfg, binary, mode=mode, startup_timeout=180, **kwargs)
        instances.append(db)
        start = time.monotonic()
        print('START ' + mode + ' runtime ' + str(len(instances)), flush=True)
        db.attach()
        report['timings_seconds'][mode + '-' + str(len(instances))] = round(time.monotonic()-start, 3)
        return db
    def rows(db, sql):
        return db.execute(sql)['rows']
    try:
        writer = database('writer')
        writer.execute('create database demo')
        writer.execute('use demo')
        writer.execute('create table customers(id bigint primary key, name varchar(30), email varchar(50) unique)')
        writer.execute('create table orders(id bigint primary key, customer_id bigint, amount decimal(12,2), foreign key(customer_id) references customers(id))')
        writer.execute("insert into customers values(1,'ada','ada@demo'),(2,'grace','grace@demo')")
        writer.execute('insert into orders values(1,1,10.50),(2,1,40.00),(3,2,100.00)')
        queries = [
            'select count(*),sum(amount) from orders',
            'select c.name,sum(o.amount) from customers c join orders o on c.id=o.customer_id group by c.name order by c.name',
            'select id, row_number() over(order by amount desc) from orders order by id',
            'select name from customers where id in(select customer_id from orders where amount>50) order by name',
        ]
        expected = [rows(writer, q) for q in queries]
        assert expected[0] == [[3, '150.50']], expected
        passed('native SQL: DDL, INSERT, JOIN, window, subquery', expected)
        for sql in ["insert into customers values(9,'dup','ada@demo')", 'insert into orders values(9,999,1.00)']:
            try:
                writer.execute(sql)
            except Exception as err:
                passed('native constraint rejection', str(err))
            else:
                raise AssertionError('constraint accepted: ' + sql)
        writer.execute('begin')
        writer.execute('insert into orders values(99,1,999.00)')
        writer.execute('rollback')
        writer.execute("select mo_ctl('dn','checkpoint','')")
        # A successful mutation publishes the checkpoint together with the WAL.
        writer.execute('update orders set amount=amount where id=1')
        assert rows(writer, queries[0]) == expected[0]
        passed('writer transaction rollback')
        reader = database('reader', database='demo')
        assert [rows(reader, q) for q in queries] == expected
        assert rows(reader, 'explain select sum(amount) from orders')
        initial = reader.runtime['version']
        passed('independent reader restores all native SQL from bucket', initial)
        for sql in ['insert into orders values(7,1,1)', "prepare blocked from 'delete from orders'", 'execute blocked']:
            try:
                reader.execute(sql)
            except Exception as err:
                passed('reader write rejected', str(err))
            else:
                if sql.startswith('prepare '):
                    continue
                raise AssertionError('reader accepted write: ' + sql)
        # Keep a transaction open while another process commits a new generation.
        reader.execute('begin')
        print('READER BEGIN', reader.in_transaction, reader.runtime['connection'].server_status, flush=True)
        assert rows(reader, queries[0]) == expected[0]
        writer.execute('begin')
        writer.execute('update orders set amount=amount+1 where customer_id=1')
        writer.execute('delete from orders where id=3')
        writer.execute('insert into orders values(4,2,200.00)')
        writer.execute('commit')
        actual = rows(reader, queries[0])
        assert actual == expected[0], {'rows':actual,'expected':expected[0],'pinned':reader.in_transaction,'initial':initial,'runtime':reader.runtime['version']}
        passed('reader transaction pins generation across writer commit')
        reader.execute('commit')
        start = time.monotonic()
        assert rows(reader, queries[0]) == [[3, '252.50']]
        report['timings_seconds']['reader-refresh'] = round(time.monotonic()-start, 3)
        passed('reader automatically sees committed update/delete/insert')
        pinned = database('reader', database='demo', snapshot=initial)
        assert rows(pinned, queries[0]) == expected[0]
        passed('explicit old snapshot remains readable')
        contender = BucketDatabase(cfg, binary, mode='writer', startup_timeout=20)
        instances.append(contender)
        try:
            contender.attach()
        except RuntimeError as err:
            assert 'active writer' in str(err), str(err)
            passed('second live writer rejected by bucket CAS lease')
        else:
            raise AssertionError('two live writers acquired database')
        pinned.close()
        reader.close()
        # Acknowledge an insert, kill without graceful shutdown/checkpoint, remove
        # every byte of that runtime's local directory, then use a new directory.
        writer.execute('create table counters(id bigint auto_increment primary key, value varchar(20))')
        writer.execute("insert into counters(value) values('before')")
        writer.execute('insert into orders values(5,1,5.00)')
        old_directory = Path(writer.runtime['directory'].name)
        report['killed_writer_pid'] = writer.runtime['process'].pid
        writer.runtime['process'].kill()
        writer.runtime['process'].wait()
        writer.close()
        assert not old_directory.exists()
        time.sleep(7)
        recovered = database('writer', database='demo')
        assert rows(recovered, queries[0]) == [[4, '257.50']]
        recovered.execute('insert into orders values(6,2,6.00)')
        recovered.execute("insert into counters(value) values('after')")
        counter_rows = rows(recovered, 'select id,value from counters order by id')
        # Native auto-increment caches may leave gaps after a crash. Recovery
        # must preserve the committed row and allocate a new, larger unique ID.
        assert counter_rows[0] == [1,'before'] and counter_rows[1][0] > 1 and counter_rows[1][1] == 'after', counter_rows
        passed('native auto-increment catalog survives bucket-only recovery')
        assert rows(recovered, queries[0]) == [[5, '263.50']]
        passed('SIGKILL, empty local directory, bucket-only writer recovery and continued writes')
        fresh = database('reader', database='demo')
        assert rows(fresh, queries[0]) == [[5, '263.50']]
        passed('fresh reader sees recovered writer commit')
        report['status'] = 'passed'
        save()
    except BaseException as err:
        report['status'] = 'failed'
        report['error'] = repr(err)
        for i, db in enumerate(instances):
            if db.runtime:
                src = Path(db.runtime['directory'].name) / 'server.log'
                if src.exists():
                    shutil.copyfile(src, evidence.with_name(evidence.stem + f'-server-{i}.log'))
        save()
        raise
    finally:
        for db in reversed(instances):
            db.close()

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--config', type=Path, required=True)
    p.add_argument('--binary', required=True)
    p.add_argument('--evidence', type=Path, default=Path('integration-result.json'))
    args = p.parse_args()
    run(json.loads(args.config.read_text()), args.binary, args.evidence)
