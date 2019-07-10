package com.licerlee.hadoop.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 接受以行为单位的记录，并转换成 KV
 * <br/>2019-6-1 22:22:22	1	39
 * =>
 * <br/>2019-6-1 39 39
 */
public class TopnMapper extends Mapper<LongWritable, Text, TKey, IntWritable> {


    // 定义输出，且在map外，因为map实在run方法循环内，这样减少gc
    // 且 map输出的key，value，是会发生序列化，变成字节数组进入buffer的
    TKey tKey = new TKey();
    IntWritable intWritable = new IntWritable();


    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 以tab作为分隔符截取
        String[] values = StringUtils.split(value.toString(), '\t');

        Date sdate = null;
        try {
            sdate = simpleDateFormat.parse(values[0]);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(sdate);

        tKey.setYear(calendar.get(Calendar.YEAR));
        tKey.setMonth(calendar.get(Calendar.MONTH) + 1);
        tKey.setYear(calendar.get(Calendar.DAY_OF_MONTH));
        tKey.setTempreture(Integer.parseInt(values[2]));

        intWritable.set(Integer.parseInt(values[2]));

        // 写入map输出缓存中
        context.write(tKey, intWritable);

    }
}
