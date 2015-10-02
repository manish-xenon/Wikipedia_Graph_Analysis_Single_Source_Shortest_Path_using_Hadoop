package manish.hadoop.TFIDF;

import manish.hadoop.TFIDF.WritableTools.TupleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by manshu on 1/21/15.
 */
public class WFCMapper extends Mapper<TupleWritable, TupleWritable, Text, TupleWritable> {

    private Text newKey = new Text();
    private TupleWritable newValue = new TupleWritable();
    //private IntWritable one = new IntWritable(1);

    public void map(TupleWritable key, TupleWritable value, Context context) throws IOException, InterruptedException {
        newKey.set(key.getFirst());
        ArrayList<Text> arrayList = new ArrayList<>();
        arrayList.add(key.getSecond());
        arrayList.add(value.getFirst());
        arrayList.add(value.getSecond());
        arrayList.add(new Text("1"));

        newValue.set(arrayList);

        context.write(newKey, newValue);
    }
}
