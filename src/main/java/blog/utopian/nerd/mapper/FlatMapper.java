package blog.utopian.nerd.mapper;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;

public class FlatMapper implements FlatMapFunction<Row, String>, Serializable {

  @Override
  public Iterator<String> call(Row row) throws Exception {
    // If no content is present in the row, then just return 'BODY_NA' to signify that user data
    // didn't have a body present in the reddit post.
    if (row.isNullAt(0)) {
      return List.of("BODY_NA").iterator();
    }

    return Arrays.asList(
            row.toString().replace("\n", "").replace("\r", "").trim().toLowerCase().split(" "))
        .iterator();
  }
}
