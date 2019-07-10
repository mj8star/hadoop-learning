package com.licerlee.hadoop.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TopnReduce extends Reducer<TKey, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(TKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {




    }
}
