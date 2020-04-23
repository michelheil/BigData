### To get dependency tree with SBT
add `addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")` 
to the file `/sparkMonitoring/project/plugins.sbt`. Then the sbt command
`dependencyTree` or better `dependencyBrowseTress` becomes available.

### To solve problem with access permission
```shell script
Exception in thread "main" org.apache.hadoop.security.AccessControlException: Permission denied: user=michael, access=WRITE, inode="/":hadoop:supergroup:drwxr-xr-x
```
sudo su - hadoop
hdfs dfs -chmod 777 /

