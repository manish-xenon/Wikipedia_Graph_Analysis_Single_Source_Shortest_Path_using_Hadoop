package manish.hadoop.Wikipedia;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by manshu on 1/24/15.
 */
public class SearchDistance extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        GraphNode node = new GraphNode(value.toString());
        String destination = context.getConfiguration().get("destination");
        String source = context.getConfiguration().get("source");
        
        if (node.getNodeId().equals(destination)) {
            String distance = node.getDistance().toString();
            if (Integer.parseInt(distance) == Integer.MAX_VALUE)
                distance = "INFINITY. No such path connecting these 2 nodes";
            context.write(new Text(source + "-----Minimum---->" + destination), new Text(distance));
        }
    }
}
