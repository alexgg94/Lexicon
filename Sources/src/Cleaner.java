import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.fieldsel.FieldSelectionHelper;
import org.apache.hadoop.mapreduce.lib.fieldsel.FieldSelectionMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Cleaner extends Configured implements Tool {
    public static class LowerCaseMapper extends Mapper<Object, Text, Text, Text> {
        private Text lowercased = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            lowercased.set(value.toString().toLowerCase());
            context.write(new Text(""), lowercased);
        }
    }

    public static class LanguageSelectorMapper extends Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            if (key.toString().equals(" u'es', ' ; '"))
                context.write(new Text(""), new Text(value.toString().substring(2, value.toString().length() - 8)));
        }
    }

    public static class TweetSelectorMapper extends Mapper<Text, Text, Text, Text> {

        private Pattern pattern = Pattern.compile("((?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+))*([A-Za-z0-9-_]+)(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)");
        private Matcher matcher;

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            matcher = pattern.matcher(value.toString());

            if (matcher.find()) {
                context.write(new Text(""), new Text(value.toString()));
            }
        }
    }

    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), Cleaner.class);

        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        fs.delete(outputPath, true);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        Job job = Job.getInstance();

        Configuration lowerCaseMapperConf = new Configuration(false);
        ChainMapper.addMapper(job, LowerCaseMapper.class, Object.class, Text.class, Text.class, Text.class, lowerCaseMapperConf);

        Configuration fieldSelectionMapperConf = new Configuration(false);
        fieldSelectionMapperConf.set(FieldSelectionHelper.DATA_FIELD_SEPERATOR, ", ' ; ',");
        fieldSelectionMapperConf.set(FieldSelectionHelper.MAP_OUTPUT_KEY_VALUE_SPEC, "2:0");
        ChainMapper.addMapper(job, FieldSelectionMapper.class, Text.class, Text.class, Text.class, Text.class, fieldSelectionMapperConf);

        Configuration languageSelectorMapperConf = new Configuration(false);
        ChainMapper.addMapper(job, LanguageSelectorMapper.class, Text.class, Text.class, Text.class, Text.class, languageSelectorMapperConf);

        Configuration tweetSelectorMapperConf = new Configuration(false);
        ChainMapper.addMapper(job, TweetSelectorMapper.class, Text.class, Text.class, Text.class, Text.class, tweetSelectorMapperConf);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Cleaner(), args);
        System.exit(exitCode);
    }
}

