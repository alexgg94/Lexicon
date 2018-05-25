package CustomTypes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OwnWritable implements Writable {
    private IntWritable polarity;
    private IntWritable tweetLength;

    public OwnWritable() {
        polarity = new IntWritable(0);
        tweetLength = new IntWritable(0);
    }

    public void setPolarity(IntWritable polarity) {
        this.polarity = polarity;
    }

    public void setTweetLength(IntWritable tweetLength) {
        this.tweetLength = tweetLength;
    }

    public IntWritable getPolarity() {
        return polarity;

    }

    public IntWritable getTweetLength() {
        return tweetLength;
    }

    public OwnWritable(IntWritable polarity, IntWritable tweetLength) {
        this.polarity = polarity;
        this.tweetLength = tweetLength;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        polarity.write(dataOutput);
        tweetLength.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        polarity.readFields(dataInput);
        tweetLength.readFields(dataInput);
    }

    @Override
    public String toString() {
        return polarity.toString() + "\t" + tweetLength.toString();
    }
}
