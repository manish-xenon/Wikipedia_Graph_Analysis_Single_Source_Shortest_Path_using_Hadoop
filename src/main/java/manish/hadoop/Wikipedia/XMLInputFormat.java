package manish.hadoop.Wikipedia;

/**
 * Created by manshu on 1/23/15.
 */
import java.io.IOException;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class XMLInputFormat extends FileInputFormat<Text, Text> {
    public static final String START_TAG_KEY = "<page>";
    public static final String END_TAG_KEY = "</page>";
    
    public static final String KEY_START_TAG = "<title>";
    public static final String KEY_END_TAG = "</title>";
    
    public static final String VALUE_START_TAG = "<text xml:space=\"preserve\">";
    public static final String VALUE_END_TAG = "</text>";


    @Override
    public RecordReader<Text, Text> createRecordReader(
            InputSplit split, TaskAttemptContext context) {
        return new XmlRecordReader();
    }

    public static class XmlRecordReader extends
            RecordReader<Text, Text> {
        private byte[] startTag;
        private byte[] endTag;
        
        private byte[] key_start_tag;
        private byte[] key_end_tag;
        private byte[] value_start_tag;
        private byte[] value_end_tag;

        private long start;
        private long end;
        private FSDataInputStream fsin;
        private DataOutputBuffer buffer = new DataOutputBuffer();
        private Text key = new Text();
        private Text value = new Text();

        @Override
        public void initialize(InputSplit is, TaskAttemptContext tac)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) is;
//            String START_TAG_KEY = "<page>";
//            String END_TAG_KEY = "</page>";
            startTag = START_TAG_KEY.getBytes("utf-8");
            endTag = END_TAG_KEY.getBytes("utf-8");
            
            key_start_tag = KEY_START_TAG.getBytes("utf-8");
            key_end_tag = KEY_END_TAG.getBytes("utf-8");
            value_start_tag = VALUE_START_TAG.getBytes("utf-8");
            value_end_tag = VALUE_END_TAG.getBytes("utf-8");

            start = fileSplit.getStart();
            end = start + fileSplit.getLength();
            Path file = fileSplit.getPath();

            FileSystem fs = file.getFileSystem(tac.getConfiguration());
            fsin = fs.open(fileSplit.getPath());
            fsin.seek(start);

        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (fsin.getPos() < end) {
                if (readUntilMatch(startTag, false)) {
                    try {
                        //buffer.write(startTag);
                        if (readUntilMatch(key_start_tag, false)) {
                            if (readUntilMatch(key_end_tag, true)) {
                                key.set(buffer.getData(), 0, buffer.getLength() - key_end_tag.length);
                            }
                            buffer.reset();
                        }
                        if (readUntilMatch(value_start_tag, false)) {
                            if (readUntilMatch(value_end_tag, true)) {
                                value.set(buffer.getData(), 0, buffer.getLength() - value_end_tag.length);
                            }
                            buffer.reset();
                        }
                        if (readUntilMatch(endTag, true)) {
                            //value.set(buffer.getData(), 0, buffer.getLength());
                            //key.set(fsin.getPos());
                            buffer.reset();
                            return true;
                        }
                    } finally {
                        buffer.reset();
                    }
                }
            }
            return false;
        }

        @Override
        public Text getCurrentKey() throws IOException,
                InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;

        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (fsin.getPos() - start) / (float) (end - start);
        }

        @Override
        public void close() throws IOException {
            fsin.close();
        }

        private boolean readUntilMatch(byte[] match, boolean withinBlock)
                throws IOException {
            int i = 0;
            while (true) {
                int b = fsin.read();

                if (b == -1)
                    return false;

                if (withinBlock)
                    buffer.write(b);

                if (b == match[i]) {
                    i++;
                    if (i >= match.length)
                        return true;
                } else
                    i = 0;

                if (!withinBlock && i == 0 && fsin.getPos() >= end)
                    return false;
            }
        }

    }

}