Dask-Spark
==========

Launch Dask from Spark and Spark from Dask.

This project is very new and experimental.  Do not use.


Examples
--------

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
