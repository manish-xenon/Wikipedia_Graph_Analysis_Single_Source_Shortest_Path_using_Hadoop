package manish.hadoop.TFIDF;

import manish.hadoop.TFIDF.WritableTools.TupleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by manshu on 1/20/15.
 */
public class WFReducer extends Reducer<TupleWritable, IntWritable, TupleWritable, IntWritable> {

    @Override
    protected void reduce(TupleWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        for (IntWritable i : values) sum += i.get();
        context.write(key, new IntWritable(sum));
        System.out.println("(" + key + "), " + sum);
    }
}
