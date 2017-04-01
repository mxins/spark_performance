package com.followeye.test;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mxins@qq.com on 2017/3/21.
 * usage: spark-submit --class com.followeye.test.JavaWordCount java/out/artifacts/test_jar/test.jar
 */
public class JavaSparkSQL {
    static String baseDir = System.getProperty("user.dir");
    static String outputDir = baseDir + "/output/";
    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL Test")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        Dataset<Row> gameDim = getGameDim(spark);
        Dataset<Row> consumeLog = getConsumeLog(spark);
        Dataset<Row> consumeAgg = getConsumeAgg(spark);
        Dataset<Row> consumeAggRaw = getConsumeAggRaw(spark);
        Dataset<Row> consumeAggUdf = getConsumeAggUdf(spark);
    };

    private static void saveDataFrame(Dataset<Row> dataFrame, String tableName) {
        dataFrame.write()
                .mode("overwrite")
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
                .csv(outputDir + tableName + "/");
    }

    private static Dataset<Row> getGameDim(SparkSession spark) {
        String path = baseDir + "/data/game_dim/";

        // The Schema is encoded in a string
        String schemaString = "game_id game_ame";

        // Generate the schema based on the string schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        // Creates DataFrame
        Dataset<Row> gameDataFrame = spark.read().schema(schema).csv(path);

        // Creates a temporary view using the DataFrame
        gameDataFrame.createOrReplaceTempView("game_dim");

        return gameDataFrame;
    }

    private static Dataset<Row> getConsumeLog(SparkSession spark) {
        String path = baseDir + "/data/consume_log/";

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("game_id", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("server_id", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("uid", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("register_date", DataTypes.DateType, true));
        fields.add(DataTypes.createStructField("date_desc", DataTypes.DateType, true));
        fields.add(DataTypes.createStructField("data_time", DataTypes.TimestampType, true));
        fields.add(DataTypes.createStructField("session", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("object", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("amount", DataTypes.FloatType, true));
        fields.add(DataTypes.createStructField("level", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("coin", DataTypes.IntegerType, true));
        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> consumeDataFrame = spark.read().schema(schema).csv(path);
        consumeDataFrame.createOrReplaceTempView("consume_log");
        return consumeDataFrame;
    }

    private static Dataset<Row> getConsumeAgg(SparkSession spark) {
        String sql = "SELECT t1.game_id,\n" +
                "       t1.server_id,\n" +
                "       t1.uid,\n" +
                "       max(t1.register_date) AS register_date,\n" +
                "       t1.date_desc,\n" +
                "       max(t1.level) AS level,\n" +
                "       SUM(t1.amount) AS amount,\n" +
                "       COUNT(1) AS consume_times\n" +
                "FROM consume_log t1\n" +
                "    JOIN game_dim t2\n" +
                "        ON t1.game_id = t2.game_id\n" +
                "GROUP BY t1.game_id,\n" +
                "         t1.server_id,\n" +
                "         t1.date_desc,\n" +
                "         t1.uid";
        Dataset<Row> consumeDataFrame = spark.sql(sql);
        consumeDataFrame.cache();
        saveDataFrame(consumeDataFrame, "consume_agg");
        return consumeDataFrame;
    }

    private static Dataset<Row> getConsumeAggRaw(SparkSession spark) {
        String sql = "SELECT t.game_id,\n" +
                "       t.server_id,\n" +
                "       t.uid, \n" +
                "       year(register_date) AS date_year, \n" +
                "       month(date_desc) AS date_month,\n" +
                "       t.level, \n" +
                "       t.amount, \n" +
                "       t.consume_times \n" +
                "FROM (SELECT t1.game_id,\n" +
                "             t1.server_id,\n" +
                "             t1.uid,\n" +
                "             max(t1.register_date) AS register_date,\n" +
                "             t1.date_desc,\n" +
                "             max(t1.level) AS level,\n" +
                "             SUM(t1.amount) AS amount,\n" +
                "             COUNT(1) AS consume_times\n" +
                "      FROM consume_log t1\n" +
                "          JOIN game_dim t2\n" +
                "              ON t1.game_id = t2.game_id\n" +
                "      GROUP BY t1.game_id,\n" +
                "               t1.server_id,\n" +
                "               t1.date_desc,\n" +
                "               t1.uid) t";
        Dataset<Row> consumeDataFrameRaw = spark.sql(sql);
        consumeDataFrameRaw.cache();
        saveDataFrame(consumeDataFrameRaw, "consume_agg_raw");
        return consumeDataFrameRaw;
    }

    private static Dataset<Row> getConsumeAggUdf(SparkSession spark) {
        SparkContext sparkContext = spark.sparkContext();
        SQLContext sqlContext = SQLContext.getOrCreate(sparkContext);
//        sqlContext.udf().registerJava("udf_year", );
        String sql = "SELECT t.game_id,\n" +
                "       t.server_id,\n" +
                "       t.uid, \n" +
                "       year(register_date) AS date_year, \n" +
                "       month(date_desc) AS date_month,\n" +
                "       t.level, \n" +
                "       t.amount, \n" +
                "       t.consume_times \n" +
                "FROM (SELECT t1.game_id,\n" +
                "             t1.server_id,\n" +
                "             t1.uid,\n" +
                "             max(t1.register_date) AS register_date,\n" +
                "             t1.date_desc,\n" +
                "             max(t1.level) AS level,\n" +
                "             SUM(t1.amount) AS amount,\n" +
                "             COUNT(1) AS consume_times\n" +
                "      FROM consume_log t1\n" +
                "          JOIN game_dim t2\n" +
                "              ON t1.game_id = t2.game_id\n" +
                "      GROUP BY t1.game_id,\n" +
                "               t1.server_id,\n" +
                "               t1.date_desc,\n" +
                "               t1.uid) t";
        Dataset<Row> consumeDataFrameUdf = spark.sql(sql);
        consumeDataFrameUdf.cache();
        saveDataFrame(consumeDataFrameUdf, "consume_agg_udf");
        return consumeDataFrameUdf;
    }
}
