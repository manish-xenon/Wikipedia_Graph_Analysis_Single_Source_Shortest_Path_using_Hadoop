package manish.hadoop.TFIDF.old_tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by manshu on 1/20/15.
 */
public class TupleRecordReader extends RecordReader<TupleWritable, Text> {
    private long start;
    private long end;
    private long pos;

    private int maxLineLength;

    private LineReader in;

    private TupleWritable key = null;

    private Text value = null;

    private ArrayList<String> outputvalues;

    /**
     * This method takes as arguments the map task’s assigned InputSplit and
     * TaskAttemptContext, and prepares the record reader. For file-based input
     * formats, this is a good place to seek to the byte position in the file to
     * begin reading.
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        outputvalues = new ArrayList<String>();
        // Retrieve configuration, and Max allowed
        // bytes for a single record
        Configuration configuration = taskAttemptContext.getConfiguration();

        FileSplit fileSplit = (FileSplit) inputSplit;

        // Split "S" is responsible for all records
        // starting from "start" and "end" positions
        start = fileSplit.getStart();
        end = start + fileSplit.getLength();

        // Retrieve file containing Split "S"
        final Path file = fileSplit.getPath();

        this.maxLineLength = configuration.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);

        // open the file and seek to the start of the split

        FileSystem fs = file.getFileSystem(configuration);

        FSDataInputStream fileIn = fs.open(fileSplit.getPath());

        // If Split "S" starts at byte 0, first line will be processed
        // If Split "S" does not start at byte 0, first line has been already
        // processed by "S-1" and therefore needs to be silently ignored
//        boolean skipFirstLine = false;
//        if (start != 0) {
//            skipFirstLine = true;
//            // Set the file pointer at "start - 1" position.
//            // This is to make sure we won't miss any line
//            // It could happen if "start" is located on a EOL
//            --start;
//            fileIn.seek(start);
//        }


        in = new LineReader(fileIn, configuration);

        if (key == null) {

            key = new TupleWritable();

        }

        if (value == null) {

            value = new Text();

        }
    }


    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (outputvalues.size() == 0) {
            Text buffer = new Text();
            int i = in.readLine(buffer);
            String str = buffer.toString();
            int tuple_start = str.indexOf("(");
            int tuple_end = str.indexOf(")");

            if (str.length() == 0 || tuple_start == -1 || tuple_end == -1) {key = null; value = null; return false;}
            System.out.println("String = " + str);

            String tuple_params[] = str.substring(tuple_start + 1, tuple_end).split(", ");
            if (tuple_params.length == 2) key.set(new Text(tuple_params[0]), new Text(tuple_params[1]));
            else {key = null; value = null; return false;}

            System.out.println(key.toString());

            tuple_params = str.substring(tuple_end + 1).split(" ");
            if (tuple_params.length == 0) {value = null;
                System.out.println("Value = NULL");
                return true;
            }
            for (String vals : tuple_params) {
                if (!vals.equals("")) {
                    outputvalues.add(vals.replaceAll("\\s+", ""));
                }
            }

            if (i == 0 || outputvalues.size() == 0) {
                key = null;
                value = null;
                return false;
            }
        }
        value.set(outputvalues.remove(0));
        System.out.println(value.toString());
        return true;
    }

    @Override
    public TupleWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0.0f;
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }

    }

}
