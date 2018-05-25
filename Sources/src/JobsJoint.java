import CustomTypes.OwnWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.fieldsel.FieldSelectionHelper;
import org.apache.hadoop.mapreduce.lib.fieldsel.FieldSelectionMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.map.RegexMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.net.URI;


public class JobsJoint {

    private static String N;

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        Path inputPath = new Path(args[0]);
        N = args[1];
        String negativeWords_inputPath = args[2];
        String positiveWords_inputPath = args[3];
        Path cleanedDataOutputPath = new Path(args[4]);
        Path topicsOutputPath = new Path(args[5]);
        Path trendingTopicsOutputPath = new Path(args[6]);
        Path hashtagSentimentOutputPath = new Path(args[7]);

        FileSystem cleanedData_fs = FileSystem.get(new URI(cleanedDataOutputPath.toString()), conf);
        cleanedData_fs.delete(cleanedDataOutputPath, true);

        FileSystem topics_fs = FileSystem.get(new URI(topicsOutputPath.toString()), conf);
        topics_fs.delete(topicsOutputPath, true);

        FileSystem trendingTopics_fs = FileSystem.get(new URI(trendingTopicsOutputPath.toString()), conf);
        trendingTopics_fs.delete(trendingTopicsOutputPath, true);

        FileSystem hashtagSentiment_fs = FileSystem.get(new URI(hashtagSentimentOutputPath.toString()), conf);
        hashtagSentiment_fs.delete(hashtagSentimentOutputPath, true);

        /*-----------------------------------------------------------------------------------------------------------*/
        Job cleaner_job = Job.getInstance(conf);
        cleaner_job.setJobName("Cleaner");

        Configuration lowerCaseMapperConf = new Configuration(false);
        ChainMapper.addMapper(cleaner_job, Cleaner.LowerCaseMapper.class, Object.class, Text.class, Text.class, Text.class, lowerCaseMapperConf);

        Configuration fieldSelectionMapperConf = new Configuration(false);
        fieldSelectionMapperConf.set(FieldSelectionHelper.DATA_FIELD_SEPERATOR, ", ' ; ',");
        fieldSelectionMapperConf.set(FieldSelectionHelper.MAP_OUTPUT_KEY_VALUE_SPEC, "2:0");
        ChainMapper.addMapper(cleaner_job, FieldSelectionMapper.class, Text.class, Text.class, Text.class, Text.class, fieldSelectionMapperConf);

        Configuration languageSelectorMapperConf = new Configuration(false);
        ChainMapper.addMapper(cleaner_job, Cleaner.LanguageSelectorMapper.class, Text.class, Text.class, Text.class, Text.class, languageSelectorMapperConf);

        Configuration tweetSelectorMapperConf = new Configuration(false);
        ChainMapper.addMapper(cleaner_job, Cleaner.TweetSelectorMapper.class, Text.class, Text.class, Text.class, Text.class, tweetSelectorMapperConf);

        cleaner_job.setOutputKeyClass(Text.class);
        cleaner_job.setOutputValueClass(Text.class);

        SequenceFileOutputFormat.setCompressOutput(cleaner_job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(cleaner_job, DefaultCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(cleaner_job, SequenceFile.CompressionType.BLOCK);

        FileInputFormat.addInputPath(cleaner_job, inputPath);
        FileOutputFormat.setOutputPath(cleaner_job, cleanedDataOutputPath);

        ControlledJob cJob1 = new ControlledJob(conf);
        cJob1.setJob(cleaner_job);

        /*-----------------------------------------------------------------------------------------------------------*/
        conf.set(RegexMapper.PATTERN, "(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)");
        Job topics_job = Job.getInstance(conf);
        topics_job.setJobName("Topics");

        topics_job.setJarByClass(TrendingTopics.class);
        topics_job.setMapperClass(RegexMapper.class);
        topics_job.setReducerClass(TrendingTopics.TrendingTopicsReducer.class);
        topics_job.setOutputKeyClass(Text.class);
        topics_job.setOutputValueClass(LongWritable.class);

        SequenceFileOutputFormat.setCompressOutput(topics_job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(topics_job, DefaultCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(topics_job, SequenceFile.CompressionType.BLOCK);

        FileInputFormat.addInputPath(topics_job, cleanedDataOutputPath);
        FileOutputFormat.setOutputPath(topics_job, topicsOutputPath);

        ControlledJob cJob2 = new ControlledJob(conf);
        cJob2.setJob(topics_job);

        /*-----------------------------------------------------------------------------------------------------------*/
        conf.set("N", N);
        Job topNPattern_job = Job.getInstance(conf);
        topNPattern_job.setJobName("TopNPattern");

        topNPattern_job.setJarByClass(TopNPattern.class);
        topNPattern_job.setMapperClass(TopNPattern.TopNMapper.class);
        topNPattern_job.setReducerClass(TopNPattern.TopNReducer.class);
        topNPattern_job.setOutputKeyClass(NullWritable.class);
        topNPattern_job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(topNPattern_job, topicsOutputPath);
        FileOutputFormat.setOutputPath(topNPattern_job, trendingTopicsOutputPath);

        ControlledJob cJob3 = new ControlledJob(conf);
        cJob3.setJob(topNPattern_job);

        /*-----------------------------------------------------------------------------------------------------------*/
        Job hashtagSentiment_job = Job.getInstance(conf);
        hashtagSentiment_job.setJobName("HashtagSentiment");

        hashtagSentiment_job.addCacheFile(new URI(negativeWords_inputPath));
        hashtagSentiment_job.addCacheFile(new URI(positiveWords_inputPath));

        hashtagSentiment_job.setJarByClass(HashtagSentiment.class);
        hashtagSentiment_job.setMapperClass(HashtagSentiment.HashtagSentimentMapper.class);
        hashtagSentiment_job.setReducerClass(HashtagSentiment.HashtagSentimentReducer.class);
        hashtagSentiment_job.setOutputKeyClass(Text.class);
        hashtagSentiment_job.setOutputValueClass(OwnWritable.class);

        FileInputFormat.addInputPath(hashtagSentiment_job, cleanedDataOutputPath);
        FileOutputFormat.setOutputPath(hashtagSentiment_job, hashtagSentimentOutputPath);

        ControlledJob cJob4 = new ControlledJob(conf);
        cJob4.setJob(hashtagSentiment_job);

        /*-----------------------------------------------------------------------------------------------------------*/
        JobControl jobctrl = new JobControl("jobctrl");
        jobctrl.addJob(cJob1);
        jobctrl.addJob(cJob2);
        jobctrl.addJob(cJob3);
        jobctrl.addJob(cJob4);
        cJob2.addDependingJob(cJob1);
        cJob3.addDependingJob(cJob2);
        cJob4.addDependingJob(cJob1);

        Thread jobRunnerThread = new Thread(new JobRunner(jobctrl));
        jobRunnerThread.start();

        while (!jobctrl.allFinished()) {
            System.out.println("Still running...");
            Thread.sleep(5000);
        }
        System.out.println("done");
        jobctrl.stop();
    }
}


class JobRunner implements Runnable {
    private JobControl control;

    public JobRunner(JobControl _control) {
        this.control = _control;
    }

    public void run() {
        this.control.run();
    }
}
