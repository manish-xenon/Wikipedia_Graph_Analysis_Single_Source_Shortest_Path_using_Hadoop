package manish.hadoop.Wikipedia;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by manshu on 1/24/15.
 */
public class GraphNode {
    
    public final static String KeyValDelim = "==========";
    public final static String NodeInfoDelim = "----@@----";
    public final static String EdgesDelim = "#@";
    
    private String nodeId;
    private Integer distance;
    private List<String> adjEdges = new ArrayList<String>();
    private String predecessor = null;

    public static enum Colors{
        White, Gray, Black
    }
    private Colors nodeColor = Colors.White;

    public GraphNode() {
        nodeId = null;
        predecessor = null;
        distance = Integer.MAX_VALUE;
        nodeColor = Colors.White;
        adjEdges = new ArrayList<String>();
    }
    
    public GraphNode(String nodeInfo) {
        String key_val[] = nodeInfo.split(KeyValDelim);
        String key = "", value = "";
        if (key_val.length < 2) {
            System.out.println(nodeInfo);
            System.out.println(key_val.length);
            throw new IllegalArgumentException("Key Values not correctly specified in String");
        }   
        key = key_val[0];
        this.nodeId = key;
        
        value = key_val[1];

        String[] nodeParams = value.split(NodeInfoDelim);
        //First is comma separated edges, then distance, then color and then predecessor
        if (nodeParams.length < 4) throw new IllegalArgumentException("Key Values not correctly specified in String");
        
        String adjNodes[] = nodeParams[0].split(EdgesDelim);
        for (String node : adjNodes) {
            if (node.length() > 0) adjEdges.add(node);
        }
        
        if (nodeParams[1].equals("Integer.MAX_VALUE"))
            this.distance = Integer.MAX_VALUE;
        else if (nodeParams[1].equals("Integer.MIN_VALUE"))
            this.distance = Integer.MIN_VALUE;
        else 
            this.distance = Integer.parseInt(nodeParams[1]);
        
        try {
            this.nodeColor = Colors.valueOf(nodeParams[2]);
        } catch (IllegalArgumentException iae) {
            System.out.println("Color Value Incorrectly specified in String");
            System.exit(1);
        }
        
        if (nodeParams.equals("NIL") || nodeParams.equals("")) this.predecessor = null;
        else this.predecessor = nodeParams[3];
    }

    public String nodeInfo() {
        StringBuffer stringBuffer = new StringBuffer();
        try {
            for (String adjNode : adjEdges) stringBuffer.append(adjNode).append(EdgesDelim);
        } catch (NullPointerException npe) {
            npe.printStackTrace();
            System.exit(1);
        }

        stringBuffer.append(NodeInfoDelim);
        if (this.distance == Integer.MAX_VALUE) stringBuffer.append("Integer.MAX_VALUE");
        else if (this.distance == Integer.MIN_VALUE) stringBuffer.append("Integer.MIN_VALUE");
        else stringBuffer.append(this.distance.toString());
        stringBuffer.append(NodeInfoDelim);

        stringBuffer.append(nodeColor.toString());

        stringBuffer.append(NodeInfoDelim);

        if (this.predecessor == null) stringBuffer.append("NIL");
        else stringBuffer.append(this.predecessor);

        return stringBuffer.toString();
    }
    
    @Override
    public String toString() {
        return this.nodeId + KeyValDelim + nodeInfo();
    }

    public Colors getNodeColor() {
        return nodeColor;
    }

    public void setNodeColor(Colors nodeColor) {
        this.nodeColor = nodeColor;
    }

    public String getPredecessor() {
        return predecessor;
    }

    public void setPredecessor(String predecessor) {
        this.predecessor = predecessor;
    }

    public List<String> getAdjEdges() {
        return adjEdges;
    }

    public void setAdjEdges(List<String> adjEdges) {
        this.adjEdges = adjEdges;
    }

    public Integer getDistance() {
        return distance;
    }

    public void setDistance(Integer distance) {
        this.distance = distance;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }
    
}

