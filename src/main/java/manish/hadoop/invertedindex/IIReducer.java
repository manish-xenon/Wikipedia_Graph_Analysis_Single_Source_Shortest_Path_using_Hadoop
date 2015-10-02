package manish.hadoop.invertedindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

/**
 * Created by manshu on 1/18/15.
 */
public class IIReducer extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();
    private HashSet<String> file_locations;
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        file_locations = new HashSet<String>();
        StringBuilder stringBuilder = new StringBuilder();
        for (Text val : values) {
            file_locations.add(val.toString());
        }

        for (String s : file_locations) stringBuilder.append(s).append(" ");
        result.set(stringBuilder.toString());
        context.write(key, result);
        System.out.println("Word = " + key + " locations = " + result.toString());
    }
}
