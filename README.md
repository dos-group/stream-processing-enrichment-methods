# Evaluation of data enrichment methods for distributed stream processing engines

**Note**: This repository is a fork of the [original repository](https://github.com/cfab20/stream-processing-enrichment-methods), but additionally includes the gathered data, plots, and associated code. It can be found in the `evaluation` directory.

## Introduction
<img src="flink_logo.png" align="left" width="100px"/>

An essential part of web applications and connected devices is the continuous generation of data. Areas such as IoT, operational business intelligence, but also fraud detection
rely on real-time processing of their generated data to react as fast as possible to undesired events or to make specific business decisions.

<br clear="left"/>

Stream processing engines (SPE) like [Apache Flink](https://flink.apache.org/) have been optimized to
process continuous streams of data on a large scale. Real-time processing is mandatory, but also high throughput is often required. This is achieved by a distributed execution of the SPE in the cloud, where new resources can easily be added and thus the cluster can be scaled up and down.

The processing of events in the SPE can become very complex depending on the use case. It can happen that additional data from an external data source, for example a database, is required for processing the events. As a consequence, latency requirements may not be achieved. Enriching the events with
additional data also leads to a direct dependency between SPE and external data source. This can cause problems, especially with increasing throughput and scaling of the SPE, since the external data source also has to be adapted accordingly.

Since use cases for enriching events in the SPE can be very specific, in this work a comprehensive evaluation about different enrichment methods is presented. The goal of this work is to find the right enrichment method for an application and to know its limitations and bottlenecks.
The focus is on latency critical applications, therefore the different enrichment methods are implemented on top of Flink and evaluated in detail. As external storages [Cassandra](https://cassandra.apache.org) is used and as messaging system [Kafka](https://kafka.apache.org/). 
For one enrichment method a [Redis](https://redis.io/) instance is used as a cache and to monitor and retrieve all Flink metrics [Prometheus](https://prometheus.io/) is deployed.

The following enrichment methods have been implemented:

- *Synchronous enrichment (sync)*: With this enrichment, a synchronous database query is performed for each event. This means that the next event can only be processed once the query has been completed.
- *Asynchronous enrichment (async)*: Here, an asynchronous database query is performed for each event. This means that the next event can already be processed, although the database query has not yet been completed.
- *Asynchronous enrichment using a cache (async-cache)*: With this method an asynchronous database query is executed and additionally a fixed number of events is kept in a local cache. The cache size depends here on the memory of a single [TaskManagers](https://nightlies.apache.org/flink/flink-docs-master/docs/concepts/flink-architecture/#taskmanagers).
- *Asynchronous enrichment using a cache with custom partition (async-cache-partition)*:
This method also uses a local cache, but the data stream is [partitioned](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/operators/overview/#custom-partitioning) beforehand so that keys are always processed on the same SubTask. Thus the cache size no longer depends on the size of a single TaskManager, but on the memory size of all TaskManagers.
- *Asynchronous enrichment using a external redis cache (async-cache-redis)*:
This method uses a Redis instance as an external cache. The cache size is therefore no longer dependent on Flink. 
- *Stream enrichment (stream)*: In this case, the entire data from the database is loaded directly into Flink and then events are enriched with a [CoProcessFunction](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/operators/process_function/).
- *Embedded machine learning model enrichment*: This enrichment method executes multiple machine learning models embedded in a streaming job under the assumption that the memory size of all models is larger than the memory capacities of all TaskManagers. Due to this assumption, a cache is used to hold a subset of the models.

## Use Case

To be able to evaluate the different enrichment methods, two representative use cases were implemented, which can be seen [here](flink-jobs/fraud-detection-app/src/main/java/org/myorg/OperatorsBase). 

## Start

### Requirements
* A kubernetes cluster. In this project, a Kubernetes cluster is set up on GCP using Ansible, but you can remove the corresponding role if not needed. In case you want to use GCP you need a project id and a service account.
* Docker on your local machine

### Setup

Set your GCP project values in `setup.sh` and run the script to replace the values in `playbook-cluster.yml` and `startup.sh`:

```bash
./setup.sh
```

To not have to install all the necessary packages locally, build a Docker file in which we perform all the following operations

```bash
docker build -t docker-ansible:1.0.0 gcloud-docker
```

Run the container instance and mount the `ansible` folder on the host machine to the `/workdir/ansible` folder in the container instance. Additionally mount your GCP service key into the container instance, by replacing `<service-key-file-path>` with your file path

```bash
docker run -it --rm \
    -v $(pwd)/ansible:/workdir/ansible \
    -v <service-key-file-path>:/workdir/my-sa.json \
    docker-ansible:1.0.0 /bin/bash
```

### Deployment

Before deploying you need to set crucial parameters like passwords and usernames for Cassandra, Redis etc. in the vars/main.yml for each Role. You also need to set the path for the Flink-Jar in [main.yml](ansible/streaming-job/vars/main.yml) before deploying. In the same file you can define the enrichment method you want to use. The asynchronous enrichment is the default.

From within the container instance run the following playbook to create the entire infrastructure including a kubernetes cluster on GCP and deploying Flink, Cassandra, etc.:

```bash
./startup.sh && ansible-playbook -vv --extra-vars cluster_state=present ansible/playbook-cluster.yml
```

After finishing run the following playbook to start the data generation and streaming job
```bash
./startup.sh && ansible-playbook -vv --extra-vars cluster_state=present ansible/playbook-streaming-job.yml
```

To delete the cluster or streaming job, set `cluster_state=absent` and run the corresponding playbook

Next, in Prometheus, the Flink metrics can be viewed. For example, the metric `myLatencyHistogram` shows the time it takes for an event to be processed by the streaming job.


