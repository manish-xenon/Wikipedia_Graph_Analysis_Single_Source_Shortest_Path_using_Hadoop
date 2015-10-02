package manish.hadoop.invertedindex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by manshu on 1/18/15.
 */
public class IIMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text file_name = new Text();
    private Text result = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String filename = fileSplit.getPath().getName();
        System.out.println(filename);

        file_name.set(filename);

        StringTokenizer stringTokenizer = new StringTokenizer(value.toString(), ". ,?!");

        while (stringTokenizer.hasMoreTokens()) {
            String word = stringTokenizer.nextToken().toLowerCase();
            result.set(word);
            context.write(result, file_name);
            System.out.println("Word = " + word + " " + file_name);
        }

    }

}
