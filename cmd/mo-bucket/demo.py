"""Run a disposable native writer/reader demo and save its credential-free config."""
import argparse
import json
import time
import uuid
from pathlib import Path
from runtime import BucketDatabase


def run(config, binary, output, freshness_ms=250):
    cfg = dict(config, prefix=config['prefix'].strip('/') + '/demo-' + str(uuid.uuid4()))
    output.parent.mkdir(parents=True, exist_ok=True)
    def emit(label, result):
        print(label + ': ' + json.dumps(result, ensure_ascii=False), flush=True)
    def wait_rows(reader, expected):
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            result = reader.execute('select count(*),sum(amount) from orders')
            if result['rows'] == expected:
                return result
            time.sleep(.05)
        reader_log = Path(reader.runtime['directory'].name) / 'server.log'
        output.with_suffix('.reader.log').write_bytes(reader_log.read_bytes())
        raise TimeoutError('reader did not observe expected rows: ' + json.dumps({'expected': expected, 'actual': result, 'prefix': cfg['prefix'], 'writer_rows': writer.execute('select id,amount from orders order by id')['rows'], 'reader_rows': reader.execute('select id,amount from orders order by id')['rows'], 'sync': reader.sync(), 'status': reader.status()}, ensure_ascii=False))
    with BucketDatabase(cfg, binary, mode='writer', startup_timeout=180) as writer:
        for sql in ['create database demo', 'use demo',
                    'create table orders(id bigint primary key,amount decimal(12,2))',
                    'insert into orders values(1,10.50),(2,20.50)']:
            writer.execute(sql)
        with BucketDatabase(cfg, binary, database='demo', freshness_ms=freshness_ms,
                            startup_timeout=180) as reader:
            connection = reader.execute('select connection_id()')['rows']
            pid = reader.runtime['process'].pid
            emit('初次读取', wait_rows(reader, [[2, '31.00']]))
            reader.execute('set @kept=42')
            reader.execute("prepare total from 'select sum(amount) from orders'")
            writer.execute('begin')
            writer.execute('update orders set amount=amount+5 where id=1')
            writer.execute('insert into orders values(3,9.00)')
            writer.execute('commit')
            emit('自动追上写者提交', wait_rows(reader, [[3, '45.00']]))
            assert reader.execute('execute total')['rows'] == [['45.00']]
            assert reader.runtime['process'].pid == pid
            assert reader.execute('select connection_id(),@kept')['rows'] == [[connection[0][0], 42]]
            reader.execute('begin')
            writer.execute('update orders set amount=amount+1 where id=2')
            assert reader.execute('select sum(amount) from orders')['rows'] == [['45.00']]
            emit('事务内保持旧版本', {'sum': '45.00'})
            reader.execute('commit')
            reader.sync()
            emit('结束事务并强制同步', wait_rows(reader, [[3, '46.00']]))
            emit('连接与会话保持', {'reader_pid': pid, 'connection_id': connection[0][0],
                                   'prepared_statement': 'total', 'session_variable': 42})
    # Keep credentials in the environment/default AWS chain, never in saved files.
    safe = {key:value for key,value in cfg.items() if key not in ['access_key','secret_key']}
    output.write_text(json.dumps(safe, ensure_ascii=False, indent=2))
    emit('可继续读取或写入的配置', {'path': str(output), 'prefix': cfg['prefix']})
    return cfg


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--output', type=Path, default=Path('work/demo-config.json'))
    parser.add_argument('--freshness-ms', type=float, default=250)
    args = parser.parse_args()
    run(json.loads(args.config.read_text()), args.binary, args.output, args.freshness_ms)
