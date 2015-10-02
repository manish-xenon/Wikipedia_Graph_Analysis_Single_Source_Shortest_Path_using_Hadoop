package manish.hadoop.TFIDF.WritableTools;

/**
 * Created by manshu on 1/21/15.
 */

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


public class TupleTupleRecordReader extends RecordReader<TupleWritable, TupleWritable> {
    private long start;
    private long end;
    private long pos;

    private int maxLineLength;

    private LineReader in;

    private TupleWritable key = null;

    private TupleWritable value = null;

    private String remainingString;

    private String start_symbol = "(";
    private String end_symbol = ")";
    private String delimiter = ",";


    /**
     * This method takes as arguments the map task’s assigned InputSplit and
     * TaskAttemptContext, and prepares the record reader. For file-based input
     * formats, this is a good place to seek to the byte position in the file to
     * begin reading.
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        remainingString = null;
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

        in = new LineReader(fileIn, configuration);

        if (key == null) {

            key = new TupleWritable();

        }

        if (value == null) {

            value = new TupleWritable();

        }
    }


    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        String str = remainingString;
        int i = 0;
        if (remainingString == null || remainingString.length() == 0) {
            Text buffer = new Text();
            i = in.readLine(buffer);
            str = buffer.toString();
        }
        if (i == 0 || str.length() == 0 || str == null) {
            key = null;
            value = null;
            return false;
        }
        int tuple_start = str.indexOf(start_symbol);
        int tuple_end = str.indexOf(end_symbol);

        if (str.length() == 0 || tuple_start == -1 || tuple_end == -1) {key = null; value = null; return false;}
        System.out.println("String = " + str);

        String str2 = str.substring(tuple_start + 1, tuple_end);

        String tuple_params[] = str2.split(delimiter);

        if (tuple_params.length != 0) {
            ArrayList<Text> tempArr = new ArrayList<>();
            for (String s : tuple_params) {
                tempArr.add(new Text(s));
            }
            key.set(tempArr);
        }
        else {key = null; value = null; return false;}

        System.out.println(key.toString());

        str = str.substring(tuple_end + 1);

        tuple_start = str.indexOf(start_symbol);
        tuple_end = str.indexOf(end_symbol);
        if (str.length() == 0 || tuple_start == -1 || tuple_end == -1) {value = null; return false;}

        str2 = str.substring(tuple_start + 1, tuple_end);

        tuple_params = str2.split(delimiter);


        if (tuple_params.length != 0) {
            ArrayList<Text> tempArr = new ArrayList<>();
            for (String s : tuple_params) {
                tempArr.add(new Text(s));
            }
            value.set(tempArr);
        }
        else {value = null; return false;}

        str = str.substring(tuple_end + 1);

        remainingString = str;

        System.out.println(value.toString());
        return true;
    }

    @Override
    public TupleWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public TupleWritable getCurrentValue() throws IOException, InterruptedException {
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

