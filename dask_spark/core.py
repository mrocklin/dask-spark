import atexit
from collections import defaultdict
import logging
import subprocess

from distributed.comm.core import parse_host_port
from distributed.utils import sync
from tornado import gen
import pyspark

logger = logging.getLogger(__name__)


def start_master(dask_scheduler=None, port=7077):
    scheme, host, _ = dask_scheduler.address.split(':')
    host = host.strip(':/')
    proc = subprocess.Popen(['start-master.sh', '--host', host,
                                                '--port', str(port)])
    master = 'spark://%s:%d' % (host, port)
    logger.info("Start Spark at %s", master)
    dask_scheduler.extensions['spark'] = proc

    @atexit.register
    def remove_spark_master():
        proc.terminate()

    return master


def start_slave(master, cores, memory, dask_worker=None):
    proc = subprocess.Popen(['start-slave.sh', master,
                             '--cores', str(cores),
                             '--memory', str(int(memory)) + 'B'])
    dask_worker.extensions['spark'] = proc
    logger.info("Start Spark Slave, pointing to %s", master)

    @atexit.register
    def remove_spark_slave():
        proc.terminate()

    return 'OK'


@gen.coroutine
def _dask_to_spark(client):
    cluster_info = yield client.scheduler.identity()
    hosts = defaultdict(lambda: {'cores': 0, 'memory': 0})

    hosts = {}
    for addr, info in cluster_info['workers'].items():
        host = info['host']
        if host not in hosts:
            hosts[host] = {'address': addr, 'cores': 0, 'memory': 0}
        hosts[host]['cores'] += info['ncores']
        hosts[host]['memory'] += info.get('memory_limit', 4e9)

    if not hosts:
        raise ValueError("No workers found")

    master = yield client._run_on_scheduler(start_master)
    worker_futures = yield [client._run(start_slave, master,
                                        cores=h['cores'],
                                        memory=int(h['memory']),
                                        workers=[h['address']])
                            for h in hosts.values()]
    sc = pyspark.SparkContext(master)
    return sc


def dask_to_spark(client):
    """ Launch Spark from a Dask cluster """
    return sync(client.loop, _dask_to_spark, client)
