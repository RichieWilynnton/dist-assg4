import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Hadoop MapReduce job to preprocess USvideos.csv.
 * Strips the publish_time field from a full ISO-8601 timestamp
 * (e.g. 2017-11-13T17:13:01.000Z) to just the date (e.g. 2017-11-13).
 *
 * Usage: hadoop jar PreprocessData.jar PreprocessData <input> <output>
 */
public class Task1PreprocessData {

    // Index of the publish_time column (0-based)
    private static final int PUBLISH_TIME_INDEX = 5;

    public static class PreprocessMapper
            extends Mapper<LongWritable, Text, NullWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Pass the header row through unchanged
            if (line.startsWith("video_id,")) {
                context.write(NullWritable.get(), new Text(line));
                return;
            }

            // Parse the CSV line, respecting double-quoted fields
            List<String> fields = parseCSVLine(line);

            if (fields.size() > PUBLISH_TIME_INDEX) {
                String publishTime = fields.get(PUBLISH_TIME_INDEX);
                // Keep only the date portion (first 10 characters: YYYY-MM-DD)
                if (publishTime.length() >= 10) {
                    fields.set(PUBLISH_TIME_INDEX, publishTime.substring(0, 10));
                }
            }

            context.write(NullWritable.get(), new Text(rebuildCSVLine(fields)));
        }

        /**
         * Parses a single CSV line into a list of raw field strings
         * (quotes are preserved so the output is lossless).
         */
        private List<String> parseCSVLine(String line) {
            List<String> fields = new ArrayList<>();
            StringBuilder current = new StringBuilder();
            boolean inQuotes = false;

            for (int i = 0; i < line.length(); i++) {
                char c = line.charAt(i);
                if (c == '"') {
                    inQuotes = !inQuotes;
                    current.append(c);
                } else if (c == ',' && !inQuotes) {
                    fields.add(current.toString());
                    current = new StringBuilder();
                } else {
                    current.append(c);
                }
            }
            fields.add(current.toString()); // last field
            return fields;
        }

        /**
         * Rebuilds a CSV line from a list of raw field strings.
         */
        private String rebuildCSVLine(List<String> fields) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < fields.size(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(fields.get(i));
            }
            return sb.toString();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: hadoop jar Task1PreprocessData.jar Task1PreprocessData <input> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Preprocess YouTube Video Statistics");
        job.setJarByClass(Task1PreprocessData.class);

        job.setMapperClass(PreprocessMapper.class);
        job.setNumReduceTasks(0); // Mapper-only job; no shuffle/reduce needed

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
