import CustomTypes.OwnWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;


public class HashtagSentiment extends Configured implements Tool {
    public static class HashtagSentimentMapper extends Mapper<Object, Text, Text, OwnWritable> {

        private Set positiveWords = new HashSet();
        private Set negativeWords = new HashSet();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] wordFiles = context.getCacheFiles();
            if (wordFiles != null && wordFiles.length == 2) {
                readFile(wordFiles[0].getPath(), false);
                readFile(wordFiles[1].getPath(), true);
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            int tweetLength = value.toString().length();
            String[] words = value.toString().split("\\s");
            List<String> hashtags = new ArrayList<>();

            int positiveWords_count = 0;
            int negativeWords_count = 0;

            Pattern pattern = Pattern.compile("(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)");

            for (String word : words) {
                if (!word.equals(""))
                    if (pattern.matcher(word).find()) {
                        hashtags.add(word);
                    } else {
                        if (positiveWords.contains(word)) {
                            positiveWords_count++;
                        } else if (negativeWords.contains(word)) {
                            negativeWords_count++;
                        }
                    }
            }

            for (String hashtag : hashtags) {
                context.write(new Text(hashtag), new OwnWritable(new IntWritable(positiveWords_count - negativeWords_count), new IntWritable(tweetLength)));
            }
        }

        private void readFile(String filePath, boolean isPositive) {
            try {
                BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));
                String word;
                while ((word = bufferedReader.readLine()) != null) {
                    if (isPositive) {
                        positiveWords.add(word.toLowerCase());
                    } else {
                        negativeWords.add(word.toLowerCase());
                    }
                }
            } catch (IOException ex) {
                System.out.println("Error while reading words");
            }
        }
    }

    public static class HashtagSentimentReducer extends Reducer<Text, OwnWritable, Text, Text> {

        public void reduce(Text key, Iterable<OwnWritable> values, Context context) throws IOException, InterruptedException {
            double ratio = 0;
            for (OwnWritable value : values) {
                ratio += ((double) value.getPolarity().get() / (double) value.getTweetLength().get());
            }

            String tmp = String.valueOf(ratio);

            context.write(key, new Text(String.valueOf(ratio)));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        String negativeWords_inputPath = args[0];
        String positiveWords_inputPath = args[1];
        Path inputPath = new Path(args[2]);
        Path outputPath = new Path(args[3]);

        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        fs.delete(outputPath, true);

        Job job = Job.getInstance(conf);

        job.addCacheFile(new URI(negativeWords_inputPath));
        job.addCacheFile(new URI(positiveWords_inputPath));

        job.setJarByClass(HashtagSentiment.class);
        job.setMapperClass(HashtagSentimentMapper.class);
        job.setReducerClass(HashtagSentimentReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(OwnWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HashtagSentiment(), args);
        System.exit(exitCode);
    }
}

