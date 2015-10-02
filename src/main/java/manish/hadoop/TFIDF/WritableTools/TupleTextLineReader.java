package manish.hadoop.TFIDF.WritableTools;

/**
 * Created by manshu on 1/21/15.
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class TupleTextLineReader extends FileInputFormat<TupleWritable, Text> {

    @Override
    public RecordReader<TupleWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new TupleTextRecordReader();
    }
}

