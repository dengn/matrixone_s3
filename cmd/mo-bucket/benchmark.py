"""Measure attach, committed-generation refresh and steady-state native SQL.

Uses a unique child prefix, preserving existing databases. Timings include S3
head checks and full native runtime setup/retirement, not just engine execution.
"""
import argparse
import json
import statistics
import time
import uuid
from pathlib import Path
from runtime import BucketDatabase


def benchmark(config, binary, repeats=3):
    config = dict(config, prefix=config['prefix'].strip('/')+'/benchmark-'+str(uuid.uuid4()))
    result = {'prefix': config['prefix'], 'refresh_seconds': [], 'write_seconds': [],
              'steady_query_seconds': []}
    writer = BucketDatabase(config, binary, mode='writer', startup_timeout=180)
    reader = BucketDatabase(config, binary, mode='reader', database='speed', startup_timeout=180)
    try:
        start = time.monotonic()
        writer.attach()
        result['writer_attach_seconds'] = time.monotonic()-start
        writer.execute('create database speed')
        writer.execute('use speed')
        writer.execute('create table amounts(id bigint primary key, value bigint)')
        writer.execute('insert into amounts values(1,10)')
        start = time.monotonic()
        reader.attach()
        result['reader_attach_seconds'] = time.monotonic()-start
        for i in range(repeats):
            start = time.monotonic()
            writer.execute('update amounts set value=value+1 where id=1')
            result['write_seconds'].append(time.monotonic()-start)
            start = time.monotonic()
            data = reader.execute('select sum(value) from amounts')
            result['refresh_seconds'].append(time.monotonic()-start)
            assert len(data['rows'])==1 and int(data['rows'][0][0])==11+i, data
            print('refresh',i,round(result['refresh_seconds'][-1],3),flush=True)
            for _ in range(5):
                start = time.monotonic()
                data = reader.execute('select sum(value) from amounts')
                result['steady_query_seconds'].append(time.monotonic()-start)
                assert len(data['rows'])==1 and int(data['rows'][0][0])==11+i, data
        for key in ['refresh_seconds','write_seconds','steady_query_seconds']:
            result[key+'_median'] = statistics.median(result[key])
        result['status'] = 'passed'
        return result
    finally:
        reader.close()
        writer.close()


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--config', type=Path, required=True)
    p.add_argument('--binary', required=True)
    p.add_argument('--repeats', type=int, default=3)
    p.add_argument('--output', type=Path, default=Path('benchmark-result.json'))
    args = p.parse_args()
    if args.repeats < 1:
        p.error('--repeats must be positive')
    result = benchmark(json.loads(args.config.read_text()),args.binary,args.repeats)
    args.output.write_text(json.dumps(result,indent=2))
    print(json.dumps(result,indent=2),flush=True)
