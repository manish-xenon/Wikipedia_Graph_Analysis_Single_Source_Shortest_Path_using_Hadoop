package manish.hadoop.TFIDF.WritableTools;

import manish.hadoop.TFIDF.DummyMapper;
import manish.hadoop.TFIDF.DummyReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by manshu on 1/20/15.
 */
public class TupleCheckDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
//
//        Job job2 = new Job(configuration, "Stage 2");
//        job2.setJarByClass(TFIDFDriver.class);
//
//        job2.setMapperClass(DummyMapper.class);
//        job2.setReducerClass(DummyReducer.class);
//
//        job2.setMapOutputKeyClass(TupleWritable.class);
//        job2.setMapOutputValueClass(IntWritable.class);
//
//        job2.setOutputKeyClass(TupleWritable.class);
//        job2.setOutputValueClass(TupleWritable.class);
//
//        // configuration should contain reference to your namenode
//        FileSystem fs = FileSystem.get(new Configuration());
//        // true stands for recursively deleting the folder you gave
//        fs.delete(new Path("/home/manshu/Templates/EXEs/SelfProjects/WikipediaAnalysis/dummytup"), true);
//
//        FileInputFormat.addInputPath(job2, new Path("/home/manshu/Templates/EXEs/SelfProjects/WikipediaAnalysis/tuple_check.txt"));
//        FileOutputFormat.setOutputPath(job2, new Path("/home/manshu/Templates/EXEs/SelfProjects/WikipediaAnalysis/dummytup"));
//
//        job2.setInputFormatClass(TextInputFormat.class);
//        job2.setOutputFormatClass(TextOutputFormat.class);
//
//        System.exit(job2.waitForCompletion(true) ? 0 : 1);

        Job job3 = new Job(configuration, "Stage 2");
        job3.setJarByClass(TupleCheckDriver.class);

        job3.setMapperClass(DummyMapper.class);
        job3.setReducerClass(DummyReducer.class);

        job3.setMapOutputKeyClass(TupleWritable.class);
        job3.setMapOutputValueClass(TupleWritable.class);

        job3.setOutputKeyClass(TupleWritable.class);
        job3.setOutputValueClass(TupleWritable.class);

        // configuration should contain reference to your namenode
        FileSystem fs = FileSystem.get(new Configuration());
        // true stands for recursively deleting the folder you gave
        fs.delete(new Path("/home/manshu/Templates/EXEs/SelfProjects/WikipediaAnalysis/dummytup2"), true);

        FileInputFormat.addInputPath(job3, new Path("/home/manshu/Templates/EXEs/SelfProjects/WikipediaAnalysis/dummytup/part-r-00000"));
        FileOutputFormat.setOutputPath(job3, new Path("/home/manshu/Templates/EXEs/SelfProjects/WikipediaAnalysis/dummytup2"));

        job3.setInputFormatClass(TupleIntLineReader.class);
        job3.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}
