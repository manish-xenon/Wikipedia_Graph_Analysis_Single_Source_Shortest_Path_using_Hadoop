package manish.hadoop.invertedindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by manshu on 1/18/15.
 */

public class RegexFilter extends Configured implements PathFilter {

    Pattern pattern;
    Configuration conf;

    @Override
    public boolean accept(Path path) {
        Matcher m = pattern.matcher(path.toString());
        System.out.println("Is path : " + path.toString() + " matching "
                + conf.get("file.pattern") + " ? , " + m.matches());
        return m.matches();
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        pattern = Pattern.compile(conf.get("file.pattern"));
    }
}