package blog.utopian.nerd;

import static blog.utopian.nerd.util.DataProcessingUtil.Constants.*;
import static blog.utopian.nerd.util.DataProcessingUtil.getSparkSession;
import static org.apache.spark.sql.functions.desc;

import blog.utopian.nerd.mapper.FlatMapper;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkBatchApplication {

  public static void main(String[] args) throws IOException {

    SparkSession sparkSession = getSparkSession("Reddit-Words-Analyzer");

    // Creating the dataset by loading the input csv data.
    Dataset<Row> redditInputDataframe =
        sparkSession
            .read()
            .format("csv")
            .option("inferSchema", true)
            .option("header", true)
            .load(INPUT_FILES_LOCATION);

    // Creating the dataset of words that should be blocked/filtered out in our computations.
    Dataset<Row> redditBlockWordsDataframe =
        sparkSession.createDataset(Arrays.asList(BLOCK_WORDS), Encoders.STRING()).toDF();

    // Performing transformations.
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

    redditInputDataframe.show();

    FileUtils.deleteDirectory(new File(OUTPUT_LOCATION));

    redditInputDataframe
        .write()
        .option("header", true)
        .option("sep", ",")
        .csv("src/main/resources/output");

    sparkSession.close();
  }
}
