package manish.hadoop.TFIDF;

import manish.hadoop.TFIDF.WritableTools.TupleWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by manshu on 1/21/15.
 */
public class FinalMapper extends Mapper<TupleWritable, TupleWritable, TupleWritable, DoubleWritable> {

    private DoubleWritable tfidf_result = new DoubleWritable(1.0);
    private final static int D = 2;

    public void map(TupleWritable key, TupleWritable value, Context context) throws IOException, InterruptedException {
        int ni = Integer.parseInt(value.getFirst().toString().trim());
        int Nd = Integer.parseInt(value.getSecond().toString().trim());
        int M = Integer.parseInt(value.getText(2).toString().trim());


        double tfi = (ni * 1.0);// / (1.0 * Nd);
        double idfi = Math.log(D / (1.0 * M)) / Math.log(10);

        tfidf_result.set(tfi * idfi);

        context.write(key, tfidf_result);
    }
}
