Dask-Spark
==========

Launch Dask from Spark and Spark from Dask.  This project is not mature.


Examples
--------

::

   pip install dask-spark

Create Spark cluster from a Dask cluster

.. code-block:: python

   >>> from dask.distributed import Client
   >>> client = Client('scheduler-address:8786')
   >>> client
   <Client: scheduler='tcp://scheduler-address:8786' processes=8 cores=64>

   >>> from dask_spark import dask_to_spark
   >>> sc = dask_to_spark(client)
   >>> sc
   <pyspark.context.SparkContext at 0x7f62fa4bb550>

Create Dask cluster from a Spark cluster

.. code-block:: python

   >>> import pyspark
   >>> sc = pyspark.SparkContext('local[4]')
   <pyspark.context.SparkContext at 0x7f8b908b0128>

   >>> from dask_spark import spark_to_dask
   >>> client = spark_to_dask(sc)
   >>> client
   <Client: scheduler="'tcp://127.0.0.1:8786'">


Requirements and How this Works
-------------------------------

This depends on a relatively recent version of Dask.distributed.

For starting Spark from Dask this assumes that you have Spark installed and
that the ``start-master.sh`` and ``start-slave.sh`` Spark scripts are available
on the PATH of the workers.  This starts a long-running Spark master process on
the Dask Scheduler and starts long running Spark slaves on Dask workers.  There
will only be one slave per worker.  We set the number of cores and the amount
of memory to match the Dask workers and available memory.

When starting Dask from Spark this will block the Spark cluster.  We start a
scheduler on the local machine and then run a long-running function that starts
up a Dask worker using ``RDD.mapPartitions``.


TODO
----

- [ ] This almost certainly fails in non-trivial situations
- [ ] Enable user specification of Java flags for memory and core use
- [ ] Support multiple spark clusters per Dask cluster
