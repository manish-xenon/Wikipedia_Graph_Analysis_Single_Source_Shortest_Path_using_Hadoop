package manish.hadoop.Wikipedia;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by manshu on 1/24/15.
 */
public class WikiBFSMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        GraphNode node = new GraphNode(line);
        String sourceId = context.getConfiguration().get("source");
        if (node.getNodeId().equals(sourceId)) {
            node.setDistance(0);
            node.setPredecessor("Source");
            node.setNodeColor(GraphNode.Colors.Gray);
        }
        
        if (node.getNodeColor() == GraphNode.Colors.Gray) {
            for (String adjNode : node.getAdjEdges()) {
                GraphNode succ_node = new GraphNode();
                succ_node.setNodeId(adjNode);
                succ_node.setNodeColor(GraphNode.Colors.Gray);
                succ_node.setDistance(node.getDistance() + 1);
                succ_node.setPredecessor(node.getNodeId());
                
                context.write(new Text(succ_node.getNodeId()), new Text(succ_node.nodeInfo()));
            }
            node.setNodeColor(GraphNode.Colors.Black);
        }
        
        context.write(new Text(node.getNodeId()), new Text(node.nodeInfo()));
    }
}
