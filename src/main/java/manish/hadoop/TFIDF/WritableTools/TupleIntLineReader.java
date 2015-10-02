package manish.hadoop.TFIDF.WritableTools;

/**
 * Created by manshu on 1/21/15.
 */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class TupleIntLineReader extends FileInputFormat<TupleWritable, IntWritable> {

    @Override
    public RecordReader<TupleWritable, IntWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new TupleIntRecordReader();
    }
}

