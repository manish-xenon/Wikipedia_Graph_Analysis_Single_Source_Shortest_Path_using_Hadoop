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


public class TupleTextRecordReader extends RecordReader<TupleWritable, Text> {
    private long start;
    private long end;
    private long pos;

    private int maxLineLength;

    private LineReader in;

    private TupleWritable key = null;

    private Text value = null;

    private ArrayList<String> outputvalues;

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
        outputvalues = new ArrayList<>();
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

            value = new Text();

        }
    }


    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (outputvalues.size() == 0) {
            Text buffer = new Text();
            int i = in.readLine(buffer);
            String str = buffer.toString();
            int tuple_start = str.indexOf(start_symbol);
            int tuple_end = str.indexOf(end_symbol);

            if (str.length() == 0 || tuple_start == -1 || tuple_end == -1) {key = null; value = null; return false;}
            System.out.println("String = " + str);

            String tuple_params[] = str.substring(tuple_start + 1, tuple_end).split(delimiter);
            if (tuple_params.length != 0) {
                ArrayList<Text> temp = new ArrayList<>();
                for (String s : tuple_params) {
                    temp.add(new Text(s));
                }
                key.set(temp);
            }
            else {key = null; value = null; return false;}

            System.out.println(key.toString());

            str = str.substring(tuple_end + 1);
            //tuple_start = str.indexOf(start_symbol);

            tuple_params = str.split(" ");
            if (tuple_params.length == 0) {value = null;
                System.out.println("Value = NULL");
                return true;
            }
            for (String vals : tuple_params) {
                if (!vals.equals("")) {
                    vals = vals.replaceAll("\\s+", "");
                    if (!vals.equals("")) outputvalues.add(vals);
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

