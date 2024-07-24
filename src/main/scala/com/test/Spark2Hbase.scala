package com.test


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.spark.{ByteArrayWrapper, FamiliesQualifiersValues, FamilyHFileWriteOptions, HBaseContext}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.HashMap
import java.util.UUID

object Spark2Hbase {
  def main(args: Array[String]): Unit = {
    //hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles /tmp/hbase/HFlileOut/ staff
    val sourceTable: String = "staff"
    val spark: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "192.168.7.169:2181")
    //  conf.setInt("zookeeper.recovery.retry", 0)
    //  conf.setInt("hbase.client.retries.number", 0)
    val hbaseContext = new HBaseContext(sc, conf)

    val cf: String = "f1"
    val conn: Connection = ConnectionFactory.createConnection(conf)
    //    val tableName: TableName = createTable(conn, sourceTable, cf)
    //    //先删除可能存在的目录
    //    delete_hdfspath(savePath.toUri.getPath)
    //一定要导入这个包才能使用hbaseBulkLoadThinRows
    import org.apache.hadoop.hbase.spark.HBaseRDDFunctions.GenericHBaseRDDFunctions
    //获取数据
    val sourceDataFrame: DataFrame = spark.sql("select * from ods.customer")
    val columnsName: Array[String] = sourceDataFrame.columns //获取所有列名
    sourceDataFrame
      .rdd
      .map(row => {
        val familyQualifiersValues: FamiliesQualifiersValues = new FamiliesQualifiersValues
        val rowkey: String = UUID.randomUUID.toString
        //对每一列进行处理
        for (i <- 0 until columnsName.length - 1) {
          try {
            familyQualifiersValues += (Bytes.toBytes(cf), Bytes.toBytes(columnsName(i)), Bytes.toBytes(row.getAs(columnsName(i)).toString))
          } catch {
            case e: ClassCastException =>
              familyQualifiersValues += (Bytes.toBytes(cf), Bytes.toBytes(columnsName(i)), Bytes.toBytes(row.getAs(columnsName(i)).toString))
            case e: Exception =>
              e.printStackTrace()
          }
        }
        (new ByteArrayWrapper(Bytes.toBytes(rowkey)), familyQualifiersValues)
      }).hbaseBulkLoadThinRows(hbaseContext, TableName.valueOf(sourceTable), t => t, "/tmp/hbase/HFlileOut", new HashMap[Array[Byte], FamilyHFileWriteOptions], compactionExclude = false, HConstants.DEFAULT_MAX_FILE_SIZE)
    spark.stop()
  }
}
