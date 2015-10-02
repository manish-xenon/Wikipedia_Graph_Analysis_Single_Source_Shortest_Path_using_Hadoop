package manish.hadoop.TFIDF;

import manish.hadoop.TFIDF.WritableTools.TupleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by manshu on 1/20/15.
 */
public class DummyReducer extends Reducer<TupleWritable, TupleWritable, TupleWritable, TupleWritable> {

    protected void reduce(TupleWritable key, Iterable<TupleWritable> values, Context context) throws IOException, InterruptedException {
        ArrayList<Text> sum = new ArrayList<>();
        for (TupleWritable val : values)
            for (int i = 0; i < val.getSize(); i++)
                sum.add(val.getText(i));

        System.out.println(key + " ------ " + sum);
        context.write(key, new TupleWritable(sum));
    }

//
//    @Override
//    protected void reduce(TupleWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//        int sum = 0;
//        for (IntWritable val : values)  sum += val.get();
//        System.out.println(sum);
//        context.write(key, key);
//    }
}
