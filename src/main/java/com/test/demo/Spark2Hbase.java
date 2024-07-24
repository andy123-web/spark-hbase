package com.test.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.spark.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.UUID;


public class Spark2Hbase {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("JavaHBaseBulkLoadExample  " + "{outputPath}");
            return;
        }

        String tableName = args[3];
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark2Hbase")
                .enableHiveSupport()
                .getOrCreate();
        Dataset<Row> ds = spark.sql(args[2]);
        String[] columnsNames = ds.columns();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", args[1]);
        JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

        JavaRDD<Pair<ByteArrayWrapper, FamiliesQualifiersValues>> myrdd = ds.toJavaRDD().map(new Function<Row, Pair<ByteArrayWrapper, FamiliesQualifiersValues>>() {
            @Override
            public Pair<ByteArrayWrapper, FamiliesQualifiersValues> call(Row row) throws Exception {
                FamiliesQualifiersValues familiesQualifiersValues = new FamiliesQualifiersValues();
                for (String columnsName : columnsNames) {
                    familiesQualifiersValues.add(Bytes.toBytes("f1"), Bytes.toBytes(columnsName), Bytes.toBytes(row.getAs(columnsName).toString()));
                }
                return new Pair(new ByteArrayWrapper(Bytes.toBytes(UUID.randomUUID().toString())), familiesQualifiersValues);
            }
        });

        hbaseContext.bulkLoadThinRows(myrdd,
                TableName.valueOf(tableName),
                new MyBulkLoadFunction(),
                args[0],
                new HashMap(),
                false,
                HConstants.DEFAULT_MAX_FILE_SIZE);

        jsc.stop();
        spark.stop();

    }


    private static class MyBulkLoadFunction implements Function<Pair<ByteArrayWrapper, FamiliesQualifiersValues>, Pair<ByteArrayWrapper, FamiliesQualifiersValues>> {
        @Override
        public Pair<ByteArrayWrapper, FamiliesQualifiersValues> call(Pair<ByteArrayWrapper, FamiliesQualifiersValues> row) throws Exception {
            return row;
        }
    }


}



