package com.test

import org.apache.spark.sql.SparkSession

object SparkHbase {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    spark.sql("select * from ods.customer limit 10").show(false)

    spark.stop()


  }

}
