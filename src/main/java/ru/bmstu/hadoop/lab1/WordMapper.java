package ru.bmstu.hadoop.lab1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.lang.String;

public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        String text = value.toString();
        String[] words = text.split(" ");

        for (int i = 0; i < words.length; i++) {
            String word = StringUtils.strip(words[i].toLowerCase(), "./?,-!():;«»*„[]…"); //"./?,-!():;"
            context.write(new Text(word), new IntWritable(1));
        }
    }
}