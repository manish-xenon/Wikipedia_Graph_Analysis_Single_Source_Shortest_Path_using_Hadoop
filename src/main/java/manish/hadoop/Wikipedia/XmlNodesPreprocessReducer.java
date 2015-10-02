package manish.hadoop.Wikipedia;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by manshu on 1/24/15.
 */
public class XmlNodesPreprocessReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        GraphNode node = new GraphNode();
        node.setNodeId(key.toString());
        GraphNode redirected = new GraphNode();
        
        for (Text nodeInfo : values) {
            GraphNode temp = new GraphNode(key.toString() + GraphNode.KeyValDelim + nodeInfo.toString());
            if (temp.getDistance() == Integer.MIN_VALUE) {
                redirected.setNodeId(temp.getAdjEdges().get(0));
            } else {
                node.setAdjEdges(temp.getAdjEdges());
                node.setNodeColor(temp.getNodeColor());
                node.setDistance(temp.getDistance());
                node.setPredecessor(temp.getPredecessor());
            }
        }
        if (redirected.getNodeId() != null) {
            redirected.setDistance(node.getDistance());
            redirected.setNodeColor(node.getNodeColor());
            redirected.setPredecessor(node.getPredecessor());
            redirected.setAdjEdges(node.getAdjEdges());
            context.write(new Text(redirected.getNodeId()), new Text(redirected.nodeInfo()));
        }
        context.write(new Text(node.getNodeId()), new Text(node.nodeInfo()));
    }
}
