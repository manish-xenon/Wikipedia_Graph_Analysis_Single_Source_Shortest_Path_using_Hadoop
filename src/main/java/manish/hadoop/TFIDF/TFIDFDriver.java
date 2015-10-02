package manish.hadoop.TFIDF;

import manish.hadoop.TFIDF.WritableTools.TupleIntLineReader;
import manish.hadoop.TFIDF.WritableTools.TupleTupleLineReader;
import manish.hadoop.TFIDF.WritableTools.TupleWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by manshu on 1/20/15.
 */
public class TFIDFDriver {
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = new Job(configuration, "Stage 1");
        job.setJarByClass(TFIDFDriver.class);

        job.setMapperClass(WFMapper.class);
        job.setMapOutputKeyClass(TupleWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(WFReducer.class);

        job.setReducerClass(WFReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(TupleWritable.class);
        job.setOutputValueClass(IntWritable.class);

        // configuration should contain reference to your namenode
        FileSystem fs = FileSystem.get(new Configuration());
        // true stands for recursively deleting the folder you gave
        fs.delete(new Path("s1_out"), true);

        FileInputFormat.addInputPath(job, new Path("tfidf_inp"));

        FileOutputFormat.setOutputPath(job, new Path("s1_out"));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        boolean result = job.waitForCompletion(true);

        Job job2 = new Job(configuration, "Stage 2");
        job2.setJarByClass(TFIDFDriver.class);

        job2.setMapperClass(WCDocsMapper.class);
        job2.setReducerClass(WCDocsReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(TupleWritable.class);

        job2.setOutputKeyClass(TupleWritable.class);
        job2.setOutputValueClass(TupleWritable.class);

        fs.delete(new Path("s2_out"), true);

        FileInputFormat.addInputPath(job2, new Path("s1_out"));
        FileOutputFormat.setOutputPath(job2, new Path("s2_out"));

        job2.setInputFormatClass(TupleIntLineReader.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        result = job2.waitForCompletion(true);

        Job job3 = new Job(configuration, "Stage 3");
        job3.setJarByClass(TFIDFDriver.class);

        job3.setMapperClass(WFCMapper.class);
        job3.setReducerClass(WFCReducer.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(TupleWritable.class);

        job3.setOutputKeyClass(TupleWritable.class);
        job3.setOutputValueClass(TupleWritable.class);

        fs.delete(new Path("s3_out"), true);

        FileInputFormat.addInputPath(job3, new Path("s2_out"));
        FileOutputFormat.setOutputPath(job3, new Path("s3_out"));

        job3.setInputFormatClass(TupleTupleLineReader.class);
        job3.setOutputFormatClass(TextOutputFormat.class);

        result = job3.waitForCompletion(true);

        Job job4 = new Job(configuration, "Stage 3");
        job4.setJarByClass(TFIDFDriver.class);

        job4.setMapperClass(FinalMapper.class);
        job4.setReducerClass(Reducer.class);

        job4.setMapOutputKeyClass(TupleWritable.class);
        job4.setMapOutputValueClass(DoubleWritable.class);

        job4.setOutputKeyClass(TupleWritable.class);
        job4.setOutputValueClass(DoubleWritable.class);

        fs.delete(new Path("tfidf_out"), true);

        FileInputFormat.addInputPath(job4, new Path("s3_out"));
        FileOutputFormat.setOutputPath(job4, new Path("tfidf_out"));

        job4.setInputFormatClass(TupleTupleLineReader.class);
        job4.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job4.waitForCompletion(true) ? 0 : 1);

    }
}
