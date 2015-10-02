package manish.hadoop.TFIDF.old_tools;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by manshu on 1/20/15.
 */
public class TupleWritable implements WritableComparable {

    private Text first;
    private Text second;

    public TupleWritable(){
        set(new Text(), new Text());
    }

    public TupleWritable(Text first, Text second) {
        set(first, second);
    }

    public void set(Text t1, Text t2) {
        first = t1;
        second = t2;
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }


    @Override
    public int hashCode() {
        return first.hashCode() * 163 + second.hashCode();
    }


    @Override
    public String toString() {
        return "(" + first.toString() + ", " + second.toString() + ")";
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }

    @Override
    public int compareTo(Object o) {
        TupleWritable t = (TupleWritable) o;
        int first_compare = first.compareTo(t.first);
        if (first_compare == 0) return second.compareTo(t.second);
        return first_compare;
    }

    @Override
    public boolean equals(Object obj) {
        TupleWritable t = (TupleWritable) obj;
        return first.equals(t.first) && second.equals(t.second);
    }
}
