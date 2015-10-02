package manish.hadoop.invertedindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by manshu on 1/18/15.
 */

public class InvertedIndex {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = new Job(configuration, "inverted-index");

        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(IIMapper.class);
        job.setReducerClass(IIReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        //FileInputFormat.setInputPathFilter(job, RegexFilter.class);
        String path1 = args[0]; String path2 = args[1];
        FileInputFormat.addInputPaths(job, path1 + "," + path2);

        //MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class);
        //MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
