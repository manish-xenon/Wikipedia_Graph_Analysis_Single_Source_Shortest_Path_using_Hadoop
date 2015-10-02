package manish.hadoop.TFIDF.old_tools;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created by manshu on 1/20/15.
 */
public class TupleLineReader extends FileInputFormat<TupleWritable, Text> {

    @Override
    public RecordReader<TupleWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new TupleRecordReader();
    }
}
