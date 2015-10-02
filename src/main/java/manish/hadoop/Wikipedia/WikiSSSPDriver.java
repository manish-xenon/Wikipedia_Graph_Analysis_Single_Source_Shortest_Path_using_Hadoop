package manish.hadoop.Wikipedia;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by manshu on 1/24/15.
 */
public class WikiSSSPDriver {
    public static enum WikiCounter {
        numIterations
    };
    
    public static Job getJob(String jobName, Configuration conf) throws IOException {
        Job job = new Job(conf, jobName);
        
        job.setJarByClass(WikiSSSPDriver.class);

        job.setMapperClass(WikiBFSMapper.class);
        job.setReducerClass(WikiBFSReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(3);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        
        return job;
    }
    
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String Ginput = "dummy/part-r-00000";
        String Goutput = "wiki_out";
        String source = "Azure";
        String destination = "Felt";

        if (args.length >= 4) {
            Ginput = args[0];            
            Goutput = args[1];
            source = args[2];
            destination = args[3];
        }

        Configuration conf = new Configuration();
        
        conf.set("source", source);
        conf.set("destination", destination);
        conf.set("mapred.textoutputformat.separator", GraphNode.KeyValDelim);
        
        Job job;
        String jobName = "Wikipedia Single Source Shortest Path";
        int iteration = 0;
        long terminationCounter = 1;
        String input = Ginput, output = Goutput;

        FileSystem fs = FileSystem.get(conf);
        
        while (terminationCounter > 0l) {
            job = getJob(jobName, conf);
            if (iteration == 0) input = Ginput;
            else  input = output;
            output = Goutput + "-" + String.valueOf(iteration);
            
            FileInputFormat.setInputPaths(job, new Path(input));
            fs.delete(new Path(output), true);
            FileOutputFormat.setOutputPath(job, new Path(output));

            job.waitForCompletion(true);

            Counters graphCounter = job.getCounters();
            Counter currentCounter = graphCounter.findCounter(WikiCounter.numIterations);
            terminationCounter = currentCounter.getValue();
            System.out.println(currentCounter.getDisplayName() + " : " + currentCounter.getValue());
            iteration++;
        }
        for (int i = 0; i < (iteration - 1); i++) {
            String temp_op = Goutput + "-" + String.valueOf(i);
            fs.delete(new Path(temp_op));
        }

        Job job2 = new Job(conf, "Find Distance");

        job2.setJarByClass(WikiSSSPDriver.class);

        job2.setMapperClass(SearchDistance.class);
        job2.setReducerClass(Reducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job2, new Path(output));
        fs.delete(new Path(Goutput), true);
        FileOutputFormat.setOutputPath(job2, new Path(Goutput));
        
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
