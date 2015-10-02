package manish.hadoop.Wikipedia;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by manshu on 1/24/15.
 */
public class WikiBFSReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        GraphNode node = new GraphNode();
        node.setNodeId(key.toString());
        
        for (Text adjNode : values) {
            GraphNode temp = new GraphNode(key.toString() + GraphNode.KeyValDelim + adjNode.toString());
            
            if (temp.getAdjEdges().size() > 0) // when the adjacency list is present
                node.setAdjEdges(temp.getAdjEdges());
            
            if (temp.getNodeColor().ordinal() > node.getNodeColor().ordinal())
                node.setNodeColor(temp.getNodeColor());
            
            if (temp.getDistance() < node.getDistance()) {
                node.setDistance(temp.getDistance());
                node.setPredecessor(temp.getPredecessor());
            }
        }
        context.write(new Text(node.getNodeId()), new Text(node.nodeInfo()));
        if (node.getNodeColor().equals(GraphNode.Colors.Gray)) {
            context.getCounter(WikiSSSPDriver.WikiCounter.numIterations).increment(1l);
        }
    }
}
