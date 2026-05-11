import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class Task2TopChannels {

    private static final int COL_CHANNEL = 3;
    private static final int COL_VIEWS   = 7;

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Task2TopChannels <input> <output>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf().setAppName("Task2 - Top Channels by Views");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(args[0]);

        String header = lines.first();
        JavaRDD<String> data = lines.filter(line -> !line.equals(header));

        // Map: (channel_title, views)
        JavaPairRDD<String, Long> channelViews = data.mapToPair(line -> {
            List<String> fields = parseCSVLine(line);
            String channel = fields.size() > COL_CHANNEL ? fields.get(COL_CHANNEL).trim() : "UNKNOWN";
            long views = 0L;
            if (fields.size() > COL_VIEWS) {
                try {
                    views = Long.parseLong(fields.get(COL_VIEWS).trim());
                } catch (NumberFormatException e) {
                    views = 0L;
                }
            }
            return new Tuple2<>(channel, views);
        });

        JavaPairRDD<String, Long> totalViews = channelViews.reduceByKey(Long::sum);

        JavaPairRDD<Long, String> swapped = totalViews.mapToPair(t -> new Tuple2<>(t._2(), t._1()));
        JavaPairRDD<Long, String> sorted = swapped.sortByKey(false); // descending

        // Format output: ("channel_title",total_views) 
        JavaRDD<String> result = sorted
                .map(t -> "(\"" + t._2() + "\"," + t._1() + ")");

        sc.parallelize(result.take(10), 1).saveAsTextFile(args[1]);
        sc.close();
    }

    /**
     * Parses a single CSV line, respecting double-quoted fields.
     * Returns raw field values (without surrounding quotes stripped,
     * to preserve embedded commas faithfully).
     */
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
