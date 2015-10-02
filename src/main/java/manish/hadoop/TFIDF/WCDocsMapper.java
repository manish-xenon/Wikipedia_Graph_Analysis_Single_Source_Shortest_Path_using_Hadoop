package manish.hadoop.TFIDF;

import manish.hadoop.TFIDF.WritableTools.TupleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by manshu on 1/20/15.
 */
public class WCDocsMapper extends Mapper<TupleWritable, IntWritable, Text, TupleWritable> {

    private TupleWritable temp;

    public void map(TupleWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
        temp = new TupleWritable(key.getFirst(), new Text(value.toString()));
        context.write(key.getSecond(), temp);
        System.out.println("Map -> " + key.getSecond().toString() + " " + temp);
    }
}
