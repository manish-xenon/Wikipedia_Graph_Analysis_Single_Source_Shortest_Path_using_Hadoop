package manish.hadoop.Wikipedia;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
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
 * Created by manshu on 1/23/15.
 */
public class ConvertXML2GraphDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        
        String Ginput = "simplewiki-latest-pages-articles.xml";
        String Goutput = "dummy";
        
        if (args.length >= 2) {
            Ginput = args[0];
            Goutput = args[1];
        }
        
        Configuration configuration = new Configuration();
        configuration.set("mapred.textoutputformat.separator", GraphNode.KeyValDelim);

        Job job = new Job(configuration, "WikipediaXML2GraphCoverter");
        job.setJarByClass(ConvertXML2GraphDriver.class);
        
        job.setMapperClass(XmlNodeInitializerMapper.class);
        job.setReducerClass(XmlNodesPreprocessReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setInputFormatClass(XMLInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(Goutput), true);
        
        FileInputFormat.addInputPath(job, new Path(Ginput));
        FileOutputFormat.setOutputPath(job, new Path(Goutput));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
