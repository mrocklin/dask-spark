import atexit
import psutil
from time import time, sleep

from dask_spark import _dask_to_spark, dask_to_spark, spark_to_dask
from distributed import Client
from distributed.utils_test import gen_cluster, loop, cluster
import pyspark
import pytest


def is_spark_proc(proc):
    return (proc.name() == 'java' and
            any('spark' in line for line in proc.cmdline()))

@atexit.register
def cleanup():
    for proc in psutil.process_iter():
        if is_spark_proc(proc):
            proc.terminate()


@pytest.fixture
def clean():
    cleanup()

    sleep(0.1)

    while any(is_spark_proc(proc) for proc in psutil.process_iter()):
        sleep(0.1)

    try:
        yield
    finally:
        sleep(1)
        cleanup()


@gen_cluster(client=True)
def test_basic(c, s, a, b):
    sc = yield _dask_to_spark(c)
    with sc:
        assert isinstance(sc, pyspark.SparkContext)

        rdd = sc.parallelize([1, 2, 3, 4])
        assert rdd.sum() == 1 + 2 + 3 + 4


def test_sync(loop, clean):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            with dask_to_spark(c) as sc:
                assert isinstance(sc, pyspark.SparkContext)

                rdd = sc.parallelize([1, 2, 3, 4])
                assert rdd.sum() == 1 + 2 + 3 + 4


def test_spark_to_dask(loop, clean):
    conf = pyspark.SparkConf()
    with pyspark.SparkContext('local[2]', conf=conf) as sc:
        client = spark_to_dask(sc, loop=loop)
        assert isinstance(client, Client)
        assert client.loop is loop
        assert client.cluster.scheduler

        start = time()
        while len(client.cluster.scheduler.workers) < 2:
            sleep(0.01)
            assert time() < start + 10


@pytest.mark.xfail(reason='unknown')
@pytest.mark.parametrize('nanny', [True, False])
def test_cleanup(loop, nanny, clean):
    with cluster(nanny=nanny) as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            with dask_to_spark(c) as sc:
                assert isinstance(sc, pyspark.SparkContext)

    sleep(1)
    for proc in psutil.process_iter():
        if 'java' == proc.name():
            assert all('Master' not in line for line in proc.cmdline())
            assert all('Worker' not in line for line in proc.cmdline())
