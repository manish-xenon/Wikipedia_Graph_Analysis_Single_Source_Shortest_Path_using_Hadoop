package manish.hadoop.TFIDF.WritableTools;

/**
 * Created by manshu on 1/20/15.
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by manshu on 1/20/15.
 */
public class TupleWritable implements WritableComparable {

    private ArrayList<Text> texts;
    private int default_size = 2;
    private int size = default_size;
    private final static String delimiter = ",";
    private final static String start_symbol = "(";
    private final static String end_symbol = ")";

    public TupleWritable(){
        super();
        new TupleWritable(default_size);
    }

    public TupleWritable(int size){
        texts = new ArrayList<>(size);
        this.size = size;
        for (int i = 0; i < size; i++)
            texts.add(new Text());
    }

    public TupleWritable(Text first, Text second) {
        texts = new ArrayList<>(2);
        texts.add(new Text(first.toString())); texts.add(new Text(second.toString()));
        this.size = 2;
    }

    public TupleWritable(ArrayList<Text> ts){
        set(ts);
    }

//    public TupleWritable(ArrayList<String> as, boolean force){ // just for different signature
//        ArrayList<Text> ts = new ArrayList<>(as.size());
//        for (int i = 0; i < as.size(); i++)
//            ts.add(new Text(as.get(i)));
//        set(ts);
//    }

    public void set(ArrayList<Text> t) {
        this.size = t.size();
        texts = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
            texts.add(new Text(t.get(i).toString()));
    }

    public void set(Text first, Text second) {
        this.size = 2;
        texts = new ArrayList<>(size);
        texts.add(new Text(first.toString()));
        texts.add(new Text(second.toString()));
    }


    public Text getText(int i) {
        if (i >= texts.size()) return texts.get(texts.size() - 1);
        return texts.get(i);
    }

    public Text getFirst() {
        if (size > 0) return texts.get(0);
        return null;
    }

    public Text getSecond() {
        if (size > 1) return texts.get(1);
        return null;
    }

    public int getSize() {return texts.size();}


    @Override
    public int hashCode() {
        int hash_code = 0;
        for (int i = 0; i < texts.size(); i++)
            hash_code = hash_code * 163 + texts.get(i).hashCode();
        return hash_code;
    }


    @Override
    public String toString() {
        if (texts.size() == 0) return "";
        StringBuilder stringBuilder = new StringBuilder(start_symbol);
        for (int i = 0; i < texts.size() - 1; i++)
            stringBuilder.append(texts.get(i).toString()).append(delimiter);
        stringBuilder.append(texts.get(texts.size() - 1)).append(end_symbol);

        return stringBuilder.toString();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeChars(start_symbol);
        for (int i = 0; i < size - 1; i++) {
            //texts.get(i).write(dataOutput);
            dataOutput.writeChars(texts.get(i).toString());
            dataOutput.writeChars(delimiter);
            //dataOutput.writeUTF(texts.get(i).toString());
        }
        dataOutput.writeChars(texts.get(size - 1).toString());
        dataOutput.writeChars(end_symbol);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        texts = new ArrayList<>();
        String str = dataInput.readLine();
        System.out.println("Read field = " + str);
        int start = str.indexOf(start_symbol);
        int end = str.indexOf(end_symbol);
        if (str.length() == 0 || start == -1 || end == -1) return;

        str = str.substring(start + 1, end);

        String words[] = str.split(delimiter);
        for (int i = 0; i < words.length; i++) {
            texts.add(new Text(words[i]));
        }
        this.size = texts.size();
    }

    @Override
    public int compareTo(Object o) {
        TupleWritable t = (TupleWritable) o;

        int comp = 0;
        for (int i = 0; i < texts.size(); i++) {
            comp = t.getText(i).compareTo(texts.get(i));
            if (comp != 0) return comp;
        }
        return comp;
    }

    @Override
    public boolean equals(Object obj) {
        TupleWritable t = (TupleWritable) obj;

        boolean comp = true;
        for (int i = 0; i < texts.size(); i++) {
            comp = comp && t.getText(i).equals(texts.get(i));
            if (!comp) return false;
        }
        return comp;

    }
}
