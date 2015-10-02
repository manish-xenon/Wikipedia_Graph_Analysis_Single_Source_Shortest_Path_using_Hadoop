package manish.hadoop.wordcount;

/**
 * Created by manshu on 1/18/15.
 */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text word = new Text();
    private IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        StringTokenizer stringTokenizer = new StringTokenizer(value.toString(), " ");
        while (stringTokenizer.hasMoreTokens())
        {
            word.set(stringTokenizer.nextToken());
            context.write(word, one);
            System.out.println("Jalatif " + key.toString() + " | " + word.toString());
        }
    }
}
