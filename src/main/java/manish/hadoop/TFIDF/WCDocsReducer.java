package manish.hadoop.TFIDF;

import manish.hadoop.TFIDF.WritableTools.TupleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by manshu on 1/20/15.
 */
public class WCDocsReducer extends Reducer<Text, TupleWritable, TupleWritable, TupleWritable> {

    @Override
    protected void reduce(Text key, Iterable<TupleWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        List<TupleWritable> cache = new ArrayList<TupleWritable>();

        Iterator<TupleWritable> iterator = values.iterator();
        TupleWritable temp0, temp;
        while(iterator.hasNext()) {
            temp = iterator.next();
            try {
                sum += Integer.parseInt(temp.getSecond().toString().trim());
            } catch (NumberFormatException nfe) {
                System.out.println("Value not correct = " + temp.getSecond().toString());
                continue;
            }
            temp0 = new TupleWritable();
            temp0.set(new Text(temp.getFirst().toString()), new Text(temp.getSecond().toString()));
            cache.add(temp0);
            System.out.println("vals = " + temp);
        }
        System.out.println(cache);
        System.out.println("Docname = " + key + " Sum = " + sum);

        TupleWritable tupleWritable1;
        TupleWritable tupleWritable2;

        Text t1, t2;
        for (TupleWritable vals : cache) {
            t1 = vals.getFirst(); t2 = key;
            tupleWritable1 = new TupleWritable(t1, t2);

            t1 = vals.getSecond(); t2 = new Text(String.valueOf(sum));
            tupleWritable2 = new TupleWritable(t1, t2);

            //temp1 = new TupleWritable(vals.getFirst(), key);
            //temp2 = new TupleWritable(new Text(vals.getSecond()), new Text(String.valueOf(sum)));

            context.write(tupleWritable1, tupleWritable2);
            System.out.println("Reducer = " + tupleWritable1 + ", " + tupleWritable2);
        }
    }
}
