import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class Task3VideosByDate {

    private static final int COL_PUBLISH_TIME = 5;

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Task3VideosByDate <input> <output>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf().setAppName("Task3 - Videos by Publish Date");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(args[0]);

        // Remove header row and any rows with missing/invalid publish_time
        String header = lines.first();
        JavaRDD<String> data = lines.filter(line -> {
            if (line.equals(header)) return false;
            List<String> f = parseCSVLine(line);
            if (f.size() <= COL_PUBLISH_TIME) return false;
            String date = f.get(COL_PUBLISH_TIME).trim();
            // Keep only well-formed YYYY-MM-DD dates
            return date.matches("\\d{4}-\\d{2}-\\d{2}");
        });

        // Map: (publish_date, 1)
        JavaPairRDD<String, Integer> dateCounts = data.mapToPair(line -> {
            List<String> fields = parseCSVLine(line);
            String date = fields.size() > COL_PUBLISH_TIME
                    ? fields.get(COL_PUBLISH_TIME).trim()
                    : "UNKNOWN";
            return new Tuple2<>(date, 1);
        });

        JavaPairRDD<String, Integer> totalByDate = dateCounts.reduceByKey(Integer::sum);

        // Swap (date, count) -> (count, date), sort descending
        JavaPairRDD<Integer, String> swapped = totalByDate.mapToPair(t -> new Tuple2<>(t._2(), t._1()));
        JavaPairRDD<Integer, String> sorted = swapped.sortByKey(false); // descending

        // Format output: (date, count) 
        JavaRDD<String> result = sorted
                .map(t -> "(" + t._2() + ", " + t._1() + ")");

        sc.parallelize(result.take(10), 1).saveAsTextFile(args[1]);
        sc.close();
    }

    private static List<String> parseCSVLine(String line) {
        List<String> fields = new java.util.ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                fields.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        fields.add(current.toString());
        return fields;
    }
}
