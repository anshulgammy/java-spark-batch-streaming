package blog.utopian.nerd;

import static blog.utopian.nerd.util.DataProcessingUtil.Constants.*;
import static blog.utopian.nerd.util.DataProcessingUtil.getSparkSession;
import static org.apache.spark.sql.functions.desc;

import blog.utopian.nerd.mapper.FlatMapper;
import java.io.File;
import java.io.IOException;
import java.util.*;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * This class can be run in a batch way, where it will read input file from the input location and
 * after processing it will write at output location. On subsequent runs, it will accommodate all
 * the incremental changes and will also preserve the history data in the final output generated.
 */
public class SparkBatchApplication {

  public static void main(String[] args) throws IOException {

    SparkSession sparkSession = getSparkSession("Reddit-Words-Analyzer-Batch");

    // Creating the dataframe by loading the provided input csv data.
    Dataset<Row> redditInputDataframe =
        sparkSession
            .read()
            .format("csv")
            .option("inferSchema", true)
            .option("header", true)
            .load(INPUT_LOCATION);

    Dataset<Row> redditExistingDataframe = null;

    StructType structType =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("value", DataTypes.StringType, false),
              DataTypes.createStructField("count", DataTypes.LongType, true)
            });

    // Creating already computed data, existing output data, if present at output location.
    if (FileUtils.isDirectory(new File(OUTPUT_LOCATION))) {
      redditExistingDataframe =
          sparkSession
              .read()
              .format("csv")
              .option("header", true)
              .schema(structType)
              .load(OUTPUT_LOCATION);

      System.out.println("Printing details of existing output data computation:");
      redditExistingDataframe.show();
      redditExistingDataframe.printSchema();

      // Caching the dataframe so that we can safely delete the file present at output location
      // later.
      redditExistingDataframe = redditExistingDataframe.cache();
    }

    // Creating the dataset of words that should be blocked/filtered out in our computations of the
    // provided input file.
    Dataset<Row> redditBlockWordsDataframe =
        sparkSession.createDataset(Arrays.asList(BLOCK_WORDS), Encoders.STRING()).toDF();

    // Performing transformations. We are considering only those reddit post bodies which were
    // posted by usernames starting with 'c' character.
    redditInputDataframe = redditInputDataframe.filter("ID like 'c%'").select("body");
    redditInputDataframe = redditInputDataframe.flatMap(new FlatMapper(), Encoders.STRING()).toDF();

    // Note: We can use exceptAll and leftanti join here. Using '.except()' will remove duplicated
    // from redditInputDataframe, which is something we don't want.
    // redditInputDataframe = redditInputDataframe.exceptAll(redditBlockWordsDataframe);
    redditInputDataframe =
        redditInputDataframe.join(
            redditBlockWordsDataframe,
            redditInputDataframe.col("value").equalTo(redditBlockWordsDataframe.col("value")),
            "leftanti");
    redditInputDataframe = redditInputDataframe.groupBy("value").count();
    redditInputDataframe = redditInputDataframe.orderBy(desc("count"));

    System.out.println("Printing details of provided new input computation:");
    redditInputDataframe.show();
    redditInputDataframe.printSchema();

    // if history data exists then consider that to prepare the union of history data and new data.
    if (redditExistingDataframe != null) {
      redditInputDataframe =
          redditInputDataframe.union(redditExistingDataframe).orderBy(desc("count"));
    }

    List<Row> rowList = redditInputDataframe.collectAsList();

    Map<String, Long> wordCountMap = new HashMap<>();
    List<Row> wordCountRowList = new ArrayList<>();

    // Preparing final key-value pairs which is sum of history and incremental data.
    if (rowList != null) {
      rowList.forEach(
          row -> {
            String value = row.getAs("value");
            Long count = row.getAs("count");

            wordCountMap.merge(value, count, Long::sum);
          });
    }

    wordCountMap.forEach(
        (key, value) ->
            wordCountRowList.add(RowFactory.create(key == null ? BODY_NA : key, value)));

    // Preparing final output dataframe which will be written as csv file at the output location.
    Dataset<Row> finalOutputDataframe =
        sparkSession.createDataFrame(wordCountRowList, structType).orderBy(desc("count"));

    // Deleting the older file present at the output location.
    FileUtils.deleteDirectory(new File(OUTPUT_LOCATION));

    // Writing new csv file at the output location.
    finalOutputDataframe
        .write()
        .option("header", true)
        .option("sep", ",")
        .csv("src/main/resources/output");

    System.out.println("Printing final dataframe data which will be written to output location:");
    finalOutputDataframe.show();

    sparkSession.close();
  }
}
