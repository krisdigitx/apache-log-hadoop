package com.acentauri;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: kshk
 * Date: 12/2/13
 * Time: 11:15 AM
 * To change this template use File | Settings | File Templates.
 */
public class wcReducer extends Reducer<Text, Text, Text, IntWritable> {

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int sum200 = 0;
        for (Text val : values) {
            if (val.toString().contains("200"))
                sum200 += val.get();
        }
        context.write(key, new IntWritable(sum200));
    }
}
