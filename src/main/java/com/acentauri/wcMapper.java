package com.acentauri;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.*;

/**
 * Created with IntelliJ IDEA.
 * User: kshk
 * Date: 12/2/13
 * Time: 11:14 AM
 * To change this template use File | Settings | File Templates.
 */

public class wcMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final static IntWritable one = new IntWritable(1);
    private Text sip = new Text();
    private Text cip = new Text();
    private Text type = new Text();
    //private String pattern =   "\\[([\\w:/]+\\s[+\\-]\\d{4})\\] (\\S) \\[(\\w+) ([\\d.]+) ([\\d.]+) (.*) (\\w+.*)\\] (\\w.*?(\\d{3})) \"([^\"]*)\" \"([^\"]*)\" (\\S) (.*) (.*)";
    //private String pattern =   "\\[([\\w:/]+\\s[+\\-]\\d{4})\\] (\\S) \\[(\\w+) ([\\d.]+) ([\\d.\\S]+) (.*) (\\w+.*)\\] (\\w.*?(\\d{3})) \"([^\"]*)\" \"([^\"]*)\" (\\S) (.*) (.*)";
    private String pattern =   "\\[([\\w:/]+\\s[+\\-]\\d{4})\\] (\\S) \\[(\\w+) ([\\d.\\S]+) ([\\d.\\S]+) ([\\w+\\S]+) (\\w+.*)\\] (.*\\S.*(\\d{3})) \"([^\"]*)\" \"([^\"]*)\" (\\S) (.*) (.*)";

    private Pattern r = Pattern.compile(pattern);


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //String line = value.toString();
        String[] entries = value.toString().split("\r?\n");
        //StringTokenizer tokenizer = new StringTokenizer(line);
        //while (tokenizer.hasMoreTokens()) {
        //    word.set(tokenizer.nextToken());
        //    context.write(word, one);
        //}
        for (int i=0, len=entries.length; i<len; i+=1){
            Matcher matcher = r.matcher(entries[i]);
            if (matcher.find()){
              //  sip.set(matcher.group(4));
              //  context.write(sip, one);
                cip.set(matcher.group(5));
                type.set(matcher.group(9));
                context.write(cip,type);

            }
            else {
                System.out.println(value.toString());
            }
        }
    }
}
