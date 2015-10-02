package manish.hadoop.TFIDF;

import manish.hadoop.TFIDF.WritableTools.TupleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by manshu on 1/20/15.
 */
public class WFMapper extends Mapper<LongWritable, Text, TupleWritable, IntWritable> {

    private IntWritable one = new IntWritable(1);
    private String file_name;
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        file_name = fileSplit.getPath().getName();

        StringTokenizer stringTokenizer = new StringTokenizer(value.toString(), ". ,?!");

        while (stringTokenizer.hasMoreTokens()) {
            String word = stringTokenizer.nextToken().toLowerCase();
            context.write(new TupleWritable(new Text(word), new Text(file_name)), one);
            //context.write(new Text(word + "," + file_name), one);
            System.out.println("(" + word + ", " + file_name + "), 1");
        }
    }
}
