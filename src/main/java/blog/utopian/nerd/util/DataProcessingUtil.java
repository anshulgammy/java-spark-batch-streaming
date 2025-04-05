package blog.utopian.nerd.util;

import org.apache.spark.sql.SparkSession;

public final class DataProcessingUtil {

  private DataProcessingUtil() {}

  public static SparkSession getSparkSession(String appName) {
    return SparkSession.builder().appName(appName).master("local").getOrCreate();
  }

  public static class Constants {
    public static final String INPUT_FILES_LOCATION = "src/main/resources/input/";
    public static final String OUTPUT_LOCATION = "src/main/resources/output";
    public static final String ARCHIVE_FILES_LOCATION =
        "/Users/anshulgautam/Downloads/archive-reddit-data/";

    public static String[] BLOCK_WORDS = {
      "[[removed]]", "[[deleted]]", "[i", "]", "-", "[", "[the", "[&gt;", "[you", "[this", "[i'm"
    };
  }
}
