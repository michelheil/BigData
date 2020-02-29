# Overview
This project show the basic usage of the Scala API to interact with HBase. For integration tests, the concept of a 
MiniCluster is applied that comes with the package `hbase-testing-util`.

# Set-up and prerequisites
* install hadoop and HBase as written [here](https://computingforgeeks.com/how-to-install-apache-hadoop-hbase-on-ubuntu/)
* execute "bash start.sh" in ~/GitHubRepositories/ToolCommands/hadoop
  
  for testing with hBaseMiniCluster ensure that Hbase, local-master-backup and local-regionservers are not running

# Remarks on Zookeeper
Source: http://hbase.apache.org/0.94/book/zookeeper.html
  
A distributed Apache HBase (TM) installation depends on a running ZooKeeper cluster. All participating nodes and clients
need to be able to access the running ZooKeeper ensemble. Apache HBase by default manages a ZooKeeper "cluster" for you.
It will start and stop the ZooKeeper ensemble as part of the HBase start/stop process. You can also manage the ZooKeeper
ensemble independent of HBase and just point HBase at the cluster it should use. To toggle HBase management of ZooKeeper,
use the `HBASE_MANAGES_ZK` variable in `conf/hbase-env.sh`. This variable, which defaults to true, tells HBase whether to
start/stop the ZooKeeper ensemble servers as part of HBase start/stop.

```scala
  val ZOOKEEPER_QUORUM = "WRITE THE ZOOKEEPER CLUSTER THAT HBASE SHOULD USE"
  conf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
```
