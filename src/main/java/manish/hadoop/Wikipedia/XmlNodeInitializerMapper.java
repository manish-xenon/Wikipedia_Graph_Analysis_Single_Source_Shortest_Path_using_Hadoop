package manish.hadoop.Wikipedia;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by manshu on 1/23/15.
 */
public class XmlNodeInitializerMapper extends Mapper<Text, Text, Text, Text> {
//    Text newText = new Text();
    ArrayList<String> urls;

    Pattern wikiArticleMention = Pattern.compile("\\[\\[([a-zA-Z_ 0-9-]+)\\]\\]", Pattern.CASE_INSENSITIVE);
    Pattern urlMention = Pattern.compile("\\{\\{\\s*citeweb\\s*\\|\\s*url=(.*?)\\|\\s*title=(.*?)\\}\\}", Pattern.CASE_INSENSITIVE);
    Pattern citationMention2 = Pattern.compile("\\{\\{\\s*Citation.*?title=(.*?)\\s*\\|\\s*url=(.*?)\\s*\\}\\}", Pattern.CASE_INSENSITIVE);

    Pattern wikiUrlMention = Pattern.compile("https?:[/]{2}en[.]wikipedia[.]org[/]wiki[/]([a-zA-Z_ 0-9-]+)", Pattern.CASE_INSENSITIVE);
    
    Pattern wikiRedirect = Pattern.compile("#redirect\\s+\\[\\[(.*?)\\]\\]", Pattern.CASE_INSENSITIVE);

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        urls = new ArrayList<>();
        String val = value.toString();
        String keyString = key.toString();

        //System.out.println(keyString + " My value = " + val);
        //System.out.println("Received = " + keyString + " " + val);
//        val = val.substring(0, 20) + "-----------------" + val.substring(val.length() - 20);
//        val = val.replaceAll("\\n", "");
//        newText.set(val);
//        
        Matcher m1 = wikiArticleMention.matcher(val);
        
        while (m1.find()) {
            String article = m1.group(1);
            if (article == null) continue;
            article = article.replaceAll("\\s+", "_");
            article = article.replaceAll("\\(", "\\[");
            article = article.replaceAll("\\)", "\\]");
            //article = article.replaceAll("[^\\p{ASCII}]", "");
            if (article.equals("")) continue;
            urls.add(article); //urls.add(new Text(article));
        }
        
        Matcher m2 = urlMention.matcher(val);
        Matcher m3;
        
        while (m2.find()) {
            String url = m2.group(1);
            String title = m2.group(2);
            m3 = wikiUrlMention.matcher(url);
            if (!m3.matches()) continue;
            if (url != null) {
                url = url.replaceAll("\\(", "\\[");
                url = url.replaceAll("\\)", "\\]");
                //url = url.replaceAll("[^\\p{ASCII}]", "");
                if (url.equals("")) continue;
                //urls.add(new Text(url));
                urls.add(url);
            }
            //if (title != null)
            //    urls.add(new Text(title)); // title of the url
        }
        
        m2 = citationMention2.matcher(val);
        while (m2.find()) {
            String url = m2.group(2);
            String title = m2.group(1);
            m3 = wikiUrlMention.matcher(url);
            if (!m3.matches()) continue;
            if (url != null) {
                url = url.replaceAll("\\(", "\\[");
                url = url.replaceAll("\\)", "\\]");
                //url = url.replaceAll("[^\\p{ASCII}]", "");
                if (url.equals("")) continue;
                //urls.add(new Text(url));
                urls.add(url);
            }
            //if (title != null)
            //    urls.add(new Text(title)); // title of the url
        }
        
        keyString = keyString.replaceAll("\\(", "\\[");
        keyString = keyString.replaceAll("\\)", "\\]");
        //keyString = keyString.replaceAll("[^\\p{ASCII}]", "");
        keyString = keyString.replaceAll("\\s+", "_");

        if (keyString.equals("")) return;
        
        String adjacents = "";
        String delim = GraphNode.EdgesDelim;
        for (String s : urls)
                adjacents += s + delim;
        if (!adjacents.equals("") && adjacents.indexOf(delim) != -1)
            adjacents = adjacents.substring(0, adjacents.length() - delim.length());

        String nodeInfo = adjacents + GraphNode.NodeInfoDelim + "Integer.MAX_VALUE" + GraphNode.NodeInfoDelim + "White" +
                GraphNode.NodeInfoDelim + "Nil";

        m2 = wikiRedirect.matcher(val);
        if (m2.matches()) {
            String new_key_string = m2.group(1);
            if (new_key_string.length() != 0 && new_key_string != null) {
                new_key_string = new_key_string.replaceAll("\\(", "\\[");
                new_key_string = new_key_string.replaceAll("\\)", "\\]");
                //new_key_string = new_key_string.replaceAll("[^\\p{ASCII}]", "");
                new_key_string = new_key_string.replaceAll("\\s+", "_");

                adjacents = keyString;
                keyString = new_key_string;

                nodeInfo = adjacents + GraphNode.NodeInfoDelim + "Integer.MIN_VALUE" + GraphNode.NodeInfoDelim + "White" +
                        GraphNode.NodeInfoDelim + "Nil";
            }
        }
        
        context.write(new Text(keyString), new Text(nodeInfo));
    }
}
