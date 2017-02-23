import atexit
import psutil

from dask_spark import _dask_to_spark, dask_to_spark
from distributed import Client
from distributed.utils_test import gen_cluster, loop, cluster
import pyspark
import pytest


@atexit.register
def cleanup():
    for proc in psutil.process_iter():
        if ('java' == proc.name() and
            any('spark' in line for line in proc.cmdline())):
            proc.terminate()


@gen_cluster(client=True)
def test_basic(c, s, a, b):
    sc = yield _dask_to_spark(c)
    assert isinstance(sc, pyspark.SparkContext)

    rdd = sc.parallelize([1, 2, 3, 4])
    assert rdd.sum() == 1 + 2 + 3 + 4


@pytest.mark.skipif(True, reason="Can only run one SparkContext?")
def test_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            sc = dask_to_spark(c)
            assert isinstance(sc, pyspark.SparkContext)

            rdd = sc.parallelize([1, 2, 3, 4])
            assert rdd.sum() == 1 + 2 + 3 + 4
