Following the example in https://sparkbyexamples.com/hbase/spark-hbase-connectors-which-one-to-use/#shc-core

On my local environment make sure to start `dfs`, `yarn` and `hbase`. This can be done by executing 
```shell script
cd ~/GitHubRepositories/ToolCommands/hadoop
bash start.sh
```

In `hbase shell` execute `create 'employee', 'person', 'address'` which creates the table
`employee` in the default namespace with the column families `person` and `address`.

Summary on Stackoverflow
https://stackoverflow.com/questions/25040709/how-to-read-from-hbase-using-spark