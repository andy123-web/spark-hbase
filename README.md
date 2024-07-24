# spark-hbase

### 创建hbase表(rowkey选择UUID)

```
create 'test','f1',{NUMREGIONS => 100, SPLITALGO => 'HexStringSplit'}
```

### 编译

```
mvn clean package
```

### 提交任务

spark和hbase不需要在同一个集群，spark所在的集群可以和hbase集群的zk正常通信即可。第一个参数path可以是s3地址，这里的地址跟hbase表hfile存储地址没有任何关系。第二个参数是zookeeper地址，多个ip用逗号分隔。第三个参数是查询sql，结尾不要有分号。第四个参数是目标hbase的表名。

```
spark-submit \
--class com.test.demo.Spark2Hbase spark-hbase-1.0-SNAPSHOT-jar-with-dependencies.jar \
path zkhost:2181 'select * from hive_table' hbase_table_name
```

### 加载生成的Hfile

在hbase集群执行如下命令

```
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles path hbase_table_name
```
