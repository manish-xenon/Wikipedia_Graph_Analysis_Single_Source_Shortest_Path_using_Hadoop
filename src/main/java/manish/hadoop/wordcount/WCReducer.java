package manish.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by manshu on 1/18/15.
 */


public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int word_count = 0;

        for (IntWritable val : values) {
            word_count += val.get();
        }
        result.set(word_count);
        context.write(key, result);
        System.out.println("Manshu " + key + " | " + word_count);
    }
}
