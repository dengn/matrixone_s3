"""Native MO query latency with delayed S3 GETs and real HTTP fault injection.

Runs against an existing local S3 endpoint via a disposable loopback proxy;
creates a unique child prefix, and never changes an existing database.
"""
import argparse
import http.client
import json
import shutil
import statistics
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit
from runtime import BucketDatabase


class DelayedS3:
    def __init__(self, endpoint, delay_ms):
        self.target = urlsplit(endpoint)
        if self.target.scheme != 'http' or self.target.path.strip('/'):
            raise ValueError('experiment proxy requires an HTTP S3 endpoint with no path prefix')
        self.delay = delay_ms / 1000
        self.failed = threading.Event()
        self.block_chunks = threading.Event()
        self.chunk_entered = threading.Event()
        self.release_chunks = threading.Event()
        self.lock = threading.Lock()
        self.gets = 0
        owner = self
        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'
            def log_message(self, *_):
                pass
            def do_GET(self):
                with owner.lock:
                    owner.gets += 1
                time.sleep(owner.delay)
                if owner.block_chunks.is_set() and '/chunks/' in self.path:
                    owner.chunk_entered.set()
                    if not owner.release_chunks.wait(5):
                        self.send_error(504, 'injected chunk stall')
                        return
                if owner.failed.is_set():
                    self.send_error(503, 'injected S3 outage')
                    return
                connection = http.client.HTTPConnection(owner.target.hostname, owner.target.port or 80, timeout=15)
                try:
                    headers = {k:v for k,v in self.headers.items() if k.lower() not in ['host','connection']}
                    connection.request('GET', self.path, headers=headers)
                    response = connection.getresponse()
                    body = response.read()
                    self.send_response(response.status)
                    for key,value in response.getheaders():
                        if key.lower() not in ['content-length','transfer-encoding','connection','server','date']:
                            self.send_header(key,value)
                    self.send_header('Content-Length', str(len(body)))
                    self.send_header('Connection','close')
                    self.end_headers()
                    self.wfile.write(body)
                except (BrokenPipeError,ConnectionResetError):
                    pass
                finally:
                    connection.close()
        self.server = ThreadingHTTPServer(('127.0.0.1',0),Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever,daemon=True)
        self.thread.start()
        self.endpoint = f'http://127.0.0.1:{self.server.server_port}'

    def close(self):
        self.release_chunks.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)


def wait_observation(reader, version=None, timeout=5):
    end = time.monotonic()+timeout
    with reader._state:
        while time.monotonic()<end:
            obs = reader._observation
            if obs and (version is None or obs['version']==version) and (time.monotonic()-obs['checked_at'])*1000<reader.freshness_ms:
                return dict(obs)
            reader._state.wait(min(.1,max(0,end-time.monotonic())))
    raise TimeoutError(reader.status())


def summary(samples):
    values=sorted(samples)
    return {'median_ms':statistics.median(values)*1000,
            'p95_ms':values[min(len(values)-1,int(len(values)*.95))]*1000,
            'count':len(values)}


def run(config,binary,output,delay_ms=30):
    cfg = dict(config,prefix=config['prefix'].strip('/')+'/network-'+str(uuid.uuid4()))
    proxy = DelayedS3(cfg['endpoint'],delay_ms)
    readcfg = dict(cfg,endpoint=proxy.endpoint)
    writer = BucketDatabase(cfg,binary,mode='writer',startup_timeout=180)
    strict = BucketDatabase(readcfg,binary,database='latency',startup_timeout=180)
    smooth = BucketDatabase(readcfg,binary,database='latency',freshness_ms=250,startup_timeout=180)
    instances=[writer,strict,smooth]
    report={'prefix':cfg['prefix'],'extra_s3_get_delay_ms':delay_ms,'freshness_window_ms':250,'checks':[]}
    def check(name):
        report['checks'].append(name)
        print('PASS',name,flush=True)
    def value(reader):
        data=reader.execute('select value from counters where id=1')
        return data['rows'][0][0],data
    try:
        writer.attach()
        for sql in ['create database latency','use latency','create table counters(id bigint primary key,value bigint)','insert into counters values(1,0)']:
            writer.execute(sql)
        strict.attach()
        smooth.attach()
        strict_samples=[]
        for _ in range(15):
            start=time.monotonic()
            assert value(strict)[0]==0
            strict_samples.append(time.monotonic()-start)
        wait_observation(smooth)
        smooth_samples=[]
        count=smooth.status()['stats']['foreground_head_checks']
        for _ in range(60):
            start=time.monotonic()
            v,data=value(smooth)
            smooth_samples.append(time.monotonic()-start)
            assert v==0 and data['freshness']['checked_age_ms']<=250
        report['strict_query']=summary(strict_samples)
        report['bounded_query']=summary(smooth_samples)
        report['hot_query_foreground_heads']=smooth.status()['stats']['foreground_head_checks']-count
        assert report['hot_query_foreground_heads']==0,report
        check('60 bounded queries use background observations with no foreground S3 HEAD')
        assert report['bounded_query']['median_ms']<report['strict_query']['median_ms']/2,report
        check('bounded warm query median excludes injected network wait')
        pid=smooth.runtime['process'].pid
        connection=smooth.execute('select connection_id()')['rows']
        smooth.execute('set @kept=42')
        smooth.execute("prepare p from 'select value from counters where id=1'")
        prepared_samples=[]
        for i in range(1,11):
            updated=writer.execute('update counters set value=value+1 where id=1')
            wait_observation(smooth,updated['snapshot'])
            start=time.monotonic()
            assert smooth.execute('execute p')['rows']==[[i]]
            prepared_samples.append(time.monotonic()-start)
            assert smooth.runtime['process'].pid==pid
        report['prefetched_update_query']=summary(prepared_samples)
        assert smooth.execute('select connection_id(),@kept')['rows']==[[connection[0][0],42]]
        check('10 prefetched updates: fresh prepared SQL, stable PID, connection and session')
        # A source update must be visible immediately in strict mode, even
        # if the background poll has not observed the new frontier yet.
        writer.execute('update counters set value=value+1 where id=1')
        assert value(strict)[0]==11
        check('strict mode keeps immediate committed visibility')
        smooth.sync()
        proxy.release_chunks.clear()
        proxy.chunk_entered.clear()
        proxy.block_chunks.set()
        writer.execute('update counters set value=value+1 where id=1')
        assert proxy.chunk_entered.wait(3),'background did not request new WAL chunk'
        start=time.monotonic()
        # The recent old version is permitted by the explicit 250 ms window.
        old,_=value(smooth)
        report['query_during_stalled_prefetch_ms']=(time.monotonic()-start)*1000
        assert old==11 and report['query_during_stalled_prefetch_ms']<delay_ms,report
        proxy.block_chunks.clear()
        proxy.release_chunks.set()
        smooth.sync()
        assert value(smooth)[0]==12
        check('stalled background WAL download does not lock a cached SQL query')
        smooth.execute('begin')
        assert value(smooth)[0]==12
        writer.execute('update counters set value=value+1 where id=1')
        time.sleep(.35)
        assert value(smooth)[0]==12
        try:
            smooth.sync()
        except RuntimeError:
            pass
        else:
            raise AssertionError('sync advanced an open transaction')
        smooth.execute('commit')
        smooth.sync()
        assert smooth.execute('execute p')['rows']==[[13]]
        check('background prefetch keeps transaction snapshot pinned')
        proxy.failed.set()
        time.sleep(.5)
        try:
            value(smooth)
        except Exception as err:
            report['outage_error_type']=type(err).__name__
        else:
            raise AssertionError('expired observation served silently during outage')
        end=time.monotonic()+3
        while not smooth.status()['background_error'] and time.monotonic()<end:
            time.sleep(.05)
        assert smooth.status()['background_error'],smooth.status()
        check('S3 outage after freshness window expires fails instead of silently returning old data')
        proxy.failed.clear()
        smooth.sync()
        assert value(smooth)[0]==13
        check('same connection resumes after S3 recovery')
        time.sleep(2.2)
        idle_heads=smooth.status()['stats']['background_head_checks']
        time.sleep(1.2)
        report['idle_heads_in_1_2_seconds']=smooth.status()['stats']['background_head_checks']-idle_heads
        assert report['idle_heads_in_1_2_seconds']<=3,report
        assert value(smooth)[0]==13
        check('idle reader backs off S3 polling and wakes on a new query')
        watcher=smooth._watcher
        smooth.close()
        assert not watcher.is_alive()
        check('close joins the background worker')
        report['status']='passed'
    except BaseException as err:
        report['status']='failed';report['error']=repr(err)
        for i,db in enumerate(instances):
            if db.runtime:
                src=Path(db.runtime['directory'].name)/'server.log'
                if src.exists():
                    shutil.copy(src,output.with_name(output.stem+f'-server-{i}.log'))
        raise
    finally:
        proxy.failed.clear()
        proxy.release_chunks.set()
        for db in reversed(instances):db.close()
        proxy.close()
        output.write_text(json.dumps(report,indent=2))
    return report


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--config',required=True,type=Path)
    p.add_argument('--binary',required=True)
    p.add_argument('--output',required=True,type=Path)
    p.add_argument('--delay-ms',type=float,default=30)
    args=p.parse_args()
    print(json.dumps(run(json.loads(args.config.read_text()),args.binary,args.output,args.delay_ms),indent=2))
