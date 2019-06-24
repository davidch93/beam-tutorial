# Beam Tutorial
Tutorial project for Apache Spark with Java 8. [Apache Beam](http://beam.apache.org/) is a unified model for defining both batch and streaming data-parallel processing pipelines, as well as a set of language-specific SDKs for constructing pipelines and Runners for executing them on distributed processing backends, including [Apache Apex](http://apex.apache.org/), [Apache Flink](http://flink.apache.org/), [Apache Spark](http://spark.apache.org/), and [Google Cloud Dataflow](http://cloud.google.com/dataflow/).

## The Beam Model
The key concepts in the Beam programming model are:
- `PCollection`: represents a collection of data, which could be bounded or unbounded in size.
- `PTransform`: represents a computation that transforms input PCollections into output PCollections.
- `Pipeline`: manages a directed acyclic graph of PTransforms and PCollections that is ready for execution.
- `PipelineRunner`: specifies where and how the pipeline should execute.

## Runners
Beam supports executing programs on multiple distributed processing backends through PipelineRunners. Currently, the following PipelineRunners are available:
- The `DirectRunner` runs the pipeline on your local machine.
- The `ApexRunner` runs the pipeline on an Apache Hadoop YARN cluster (or in embedded mode).
- The `DataflowRunner` submits the pipeline to the [Google Cloud Dataflow](http://cloud.google.com/dataflow/).
- The `FlinkRunner` runs the pipeline on an Apache Flink cluster. The code has been donated from [dataArtisans/flink-dataflow](https://github.com/dataArtisans/flink-dataflow) and is now part of Beam.
- The `SparkRunner` runs the pipeline on an Apache Spark cluster. The code has been donated from [cloudera/spark-dataflow](https://github.com/cloudera/spark-dataflow) and is now part of Beam.

## Example Case
1. [Simple Operation](https://github.com/davidch93/beam-tutorial/tree/master/beam-simple-operation#simple-operation-tutorial)
   <br/>Apache Beam simple operation on Pipeline tutorial.
2. [Kafka Write JSON to GCS](https://github.com/davidch93/beam-tutorial/tree/master/beam-kafka-write-json-to-gcs#kafka-write-json-to-gcs-tutorial)
   <br/>Apache Beam streaming application from Kafka, transform the record, and store it to GCS with JSON format tutorial.
3. [Kafka Write Parquet to GCS](https://github.com/davidch93/beam-tutorial/tree/master/beam-kafka-write-parquet-to-gcs#kafka-write-parquet-to-gcs-tutorial)
   <br/>Apache Beam streaming application from Kafka, transform the record, and store it to GCS with Parquet format tutorial.
