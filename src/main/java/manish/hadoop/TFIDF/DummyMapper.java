package manish.hadoop.TFIDF;

import manish.hadoop.TFIDF.WritableTools.TupleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by manshu on 1/20/15.
 */
public class DummyMapper extends Mapper<TupleWritable, IntWritable, TupleWritable, TupleWritable> {

    private IntWritable one = new IntWritable(1);

    public void map(TupleWritable key, IntWritable val, Context context) throws IOException, InterruptedException {

        System.out.println(key + " ---- " + val);
        String temp = String.valueOf(val.get());
        Text value = new Text(temp);
        TupleWritable tupleWritable = new TupleWritable(value, value);
        context.write(key, tupleWritable);
    }
//    public void map(LongWritable offset, Text val, Context context) throws IOException, InterruptedException {
//        TupleWritable tuple;
//        ArrayList<String> tarr = new ArrayList<>();
//
//        String key = String.valueOf(offset);
//        String value = val.toString();
//
////        tarr.add(key); tarr.add(new Text(key.toString() + "-1")); tarr.add(new Text(key.toString() + "-2")); tarr.add(new Text(key.toString() + "-3"));
////        tarr.add(value); tarr.add(new Text(value.toString() + " 1")); tarr.add(new Text(value.toString() + " 2")); tarr.add(new Text(value.toString() + " 3"));
//        tarr.add(key);tarr.add(key + "-1");tarr.add(key + "-2");
//        tarr.add(value);tarr.add(value + " 1");tarr.add(value + " 2");
//
//        tuple = new TupleWritable(tarr);
//
//        context.write(tuple, one);
//        System.out.println("Map = " + tuple);
//    }
//    public void map(TupleWritable key, Text value, Context context) throws IOException, InterruptedException {
//
//        System.out.println("Dummy Mapper Key = " + key + " Value = " + value.toString());
//        context.write(new Text("1"), new Text("2"));
//    }
}
