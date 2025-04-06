package blog.utopian.nerd;

import static blog.utopian.nerd.util.DataProcessingUtil.getSparkSession;
import static org.apache.spark.sql.functions.desc;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class SparkStreamingApplication {

  public static void main(String[] args) throws TimeoutException, StreamingQueryException {

    SparkSession sparkSession = getSparkSession("Reddit-Words-Analyzer-Streaming");

    // Creating dataframe by reading from socket.
    // Socket has been used just of sake of simplicity. We can plug in S3 bucket location or Kafka
    // topic in future by changing the format option.
    // Start socket terminal in local using this command: nc -lk 9999
    Dataset<Row> inputStreamDataFrame =
        sparkSession
            .readStream()
            .format("socket")
            .option("host", "localhost")
            .option("port", 9999)
            .load();
    Dataset<Row> wordCountStreamDataFrame =
        inputStreamDataFrame
            .as(Encoders.STRING())
            .flatMap(
                (FlatMapFunction<String, String>)
                    input -> Arrays.asList(input.toLowerCase().split(" ")).iterator(),
                Encoders.STRING())
            .toDF();

    wordCountStreamDataFrame =
        wordCountStreamDataFrame.groupBy("value").count().orderBy(desc("count"));

    // We can output the read data in these modes: append, update, complete.
    // append: Only new appended data will be outputed.
    // update: Only updated data will be outputed. The updated data will also include history data
    // if it was part of the update.
    // complete: All the history and incremental updated data will be outputed.
    StreamingQuery streamingQuery =
        wordCountStreamDataFrame.writeStream().outputMode("complete").format("console").start();

    // This will keep the application running and always listening to the incoming data.
    streamingQuery.awaitTermination();

    sparkSession.close();
  }
}
