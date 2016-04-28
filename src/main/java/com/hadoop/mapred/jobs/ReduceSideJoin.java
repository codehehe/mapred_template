package com.hadoop.mapred.jobs;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.List;

/**
 * User: xushuai
 * Date: 16/4/28
 * Time: 下午4:58
 */
public class ReduceSideJoin {
    /*
    * left:uid|name|sex|age
    *
    * right:uid|movie|star
    *
    * result: name|movie|star
    * */
    public static class ReduceSideJoinMapper extends Mapper<Object, Text, Text, Text> {

        private Text joinKey = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            String line = value.toString();
            String[] data = line.split("|");
            joinKey.set(data[0]);
            if (data == null || data.length != 3) {
                return;
            }

            if(filename.equals("user")){
                context.write(joinKey, new Text("user_" + data[1]));
            } else if (filename.equals("star")) {
                context.write(joinKey, new Text("star_" + data[1] + "|" + data[2]));
            }
        }
    }

    public static class ReduceSideJoinReducer extends Reducer<Text, Text, Text, Text> {
        List<Text> leftTable = Lists.newArrayList();
        List<Text> rightTable = Lists.newArrayList();
        private Text output = new Text();



        public void reduce(Text key, Text value, Context context) throws IOException, InterruptedException {
            leftTable.clear();
            rightTable.clear();
            String data = value.toString();
            if(data.startsWith("user_")){
                leftTable.add(new Text(data.split("user_")[1]));
            } else if (data.startsWith("star_")) {
                leftTable.add(new Text(data.split("star_")[1]));
            }

            for(Text left: leftTable){
                for (Text right: rightTable) {
                    output.set(left.toString() + "|" + right.toString());
                    context.write(key, output);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: reduce_side_join <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "reduce_side_join");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(ReduceSideJoinMapper.class);
        job.setCombinerClass(ReduceSideJoinReducer.class);
        job.setReducerClass(ReduceSideJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
