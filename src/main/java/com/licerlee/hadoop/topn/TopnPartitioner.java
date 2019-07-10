package com.licerlee.hadoop.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 分区器
 * <br/> 目的：根据map的 KV 计算得到KVP,相同的k计算得到的p应一样
 * <br/> 2019-6-1 22:22:22	1	39
 */
public class TopnPartitioner extends Partitioner<TKey, IntWritable> {



    //1,不能太复杂。。。
    //partitioner  按  年，月  分区  -》  分区 > 分组  按 年分区！！！！！！
    //分区器潜台词：满足  相同的key获得相同的分区号就可以~！
    @Override
    public int getPartition(TKey tKey, IntWritable intWritable, int numPartitions) {

        //数据倾斜?
        return tKey.getYear() % numPartitions;
    }
}
