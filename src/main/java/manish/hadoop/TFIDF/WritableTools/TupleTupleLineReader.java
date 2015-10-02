package manish.hadoop.TFIDF.WritableTools;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created by manshu on 1/21/15.
 */
public class TupleTupleLineReader extends FileInputFormat<TupleWritable, TupleWritable> {

    @Override
    public RecordReader<TupleWritable, TupleWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new TupleTupleRecordReader();
    }
}
