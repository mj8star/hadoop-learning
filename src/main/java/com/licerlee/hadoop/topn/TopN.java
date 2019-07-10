package com.licerlee.hadoop.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 需求：
 * 求得每月气温最高的两天数据
 * <p>
 * 2019-6-1 22:22:22	1	39<br/>
 * 2019-5-21 22:22:22	3	33<br/>
 * 2019-6-1 22:22:22	1	38<br/>
 * 2019-6-2 22:22:22	2	31<br/>
 * 2018-3-11 22:22:22	3	18<br/>
 * 2018-4-23 22:22:22	1	22<br/>
 * 1970-8-23 22:22:22	2	23<br/>
 * 1970-8-8 22:22:22	1	32<br/>
 * </p>
 */
public class TopN {

    public static void main(String[] args) throws Exception {

        // 1.加载配置（true使用本地默认配置）
        Configuration configuration = new Configuration(true);
        Job job = Job.getInstance(configuration);

        // 自动注入 -D参数，且分离普通参数
        String[] remainingArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();

        // 设置job class,用于集群环境反射生成job实例
        job.setJarByClass(TopN.class);
        job.setJobName("top n job");


        // 2.设置输入、输出

        // 输入数据源目录（可以多个）
        TextInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        Path output = new Path(remainingArgs[1]);

        // 输出目录存在，先删除
        if(output.getFileSystem(configuration).exists(output)) {
            // 递归删除
            output.getFileSystem(configuration).delete(output, true);
        }
        // 输出目录，一个
        TextOutputFormat.setOutputPath(job, output);


        // 3.设置map任务
        job.setMapperClass(TopnMapper.class);
        // 设置map输出格式
        job.setMapOutputKeyClass(TKey.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置分区器, 用于按业务自定义分区，相同的key一定会分到一个partition
        job.setPartitionerClass(TopnPartitioner.class);

        // 设置排序比较器，根据业务需要，实现两个对象的大小的比较
        job.setSortComparatorClass(TopnSortComparator.class);

        // 设置combine 调优用，根据业务需要
//        job.setCombinerClass(TopnCombine.class);



        // 5.设置reduce任务
        // 分组比较器,
        job.setGroupingComparatorClass(TopnGroupingComparator.class);
        job.setReducerClass(TopnReduce.class);

        //设置reduce输出格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6.执行job, true=print the progress to the user
        job.waitForCompletion(true);

    }
}
