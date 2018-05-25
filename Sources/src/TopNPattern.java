import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
import java.util.TreeMap;


public class TopNPattern extends Configured implements Tool {

    public static class TopNMapper extends Mapper<Object, Text, NullWritable, Text> {
        private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
        private int N;
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            N = Integer.parseInt(conf.get("N"));
            String[] splitted_value = value.toString().split("\\s");

            repToRecordMap.put(Integer.parseInt(splitted_value[2]), new Text(splitted_value[1] +
                    "\t" + splitted_value[2]));

            if (repToRecordMap.size() > 2 * N) {
                repToRecordMap.remove(repToRecordMap.firstKey());
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Text text : repToRecordMap.values()) {
                context.write(NullWritable.get(), text);
            }
        }
    }

    public static class TopNReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
        private int N;
        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            N = Integer.parseInt(conf.get("N"));
            for (Text value : values) {
                String[] splitted_value = value.toString().split("\\s");

                repToRecordMap.put(Integer.parseInt(splitted_value[1]), new Text(splitted_value[0] +
                        "\t" + splitted_value[1]));

                if (repToRecordMap.size() > N) {
                    repToRecordMap.remove(repToRecordMap.firstKey());
                }
            }

            for (Text text : repToRecordMap.descendingMap().values()) {
                context.write(NullWritable.get(), text);
            }
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        conf.set("N", args[0]);
        Path inputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        fs.delete(outputPath, true);

        Job job = Job.getInstance(conf);

        job.setJarByClass(TopNPattern.class);
        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TopNPattern(), args);
        System.exit(exitCode);
    }
}

