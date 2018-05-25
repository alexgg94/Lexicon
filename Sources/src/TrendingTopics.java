import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.map.RegexMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;


public class TrendingTopics extends Configured implements Tool {
    public static class TrendingTopicsReducer
            extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values,
                           Context context) throws IOException, InterruptedException {
            int total = 0;
            for (LongWritable val : values) {
                total += Integer.valueOf(val.toString());
            }
            context.write(key, new LongWritable(total));
        }
    }

    public static class TrendingTopicsPartition extends Partitioner<Text, LongWritable> {

        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            final char TopFirstPartition='g', TopSecondPartition='p';

            String word = key.toString();
            word = word.replaceAll("\\s", "");
            char letter;
            if (!word.isEmpty())
                letter = word.toLowerCase().charAt(1);
            else return (0);
            if (numPartitions != 3) {
                return (int) (letter - 'a') % numPartitions;
            } else {
                if (letter <= TopFirstPartition)
                    return (0);
                else if (letter > TopFirstPartition && letter <= TopSecondPartition)
                    return (1);
                else if (letter > TopSecondPartition)
                    return (2);
            }
            return(0);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        fs.delete(outputPath, true);

        conf.set(RegexMapper.PATTERN, "(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)");
        Job job = Job.getInstance(conf);

        job.setJarByClass(TrendingTopics.class);
        job.setMapperClass(RegexMapper.class);
        job.setReducerClass(TrendingTopicsReducer.class);
        job.setCombinerClass(TrendingTopicsReducer.class);
        job.setPartitionerClass(TrendingTopicsPartition.class);
        job.setNumReduceTasks(3);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TrendingTopics(), args);
        System.exit(exitCode);
    }
}

