package com.hadoop.mapred.driver;

import com.hadoop.mapred.jobs.ReduceSideJoin;
import com.hadoop.mapred.jobs.WordCount;
import org.apache.hadoop.util.ProgramDriver;

/**
 * User: xushuai
 * Date: 16/4/27
 * Time: 下午6:11
 */
public class MainDriver {
    public static void main(String argv[]){
        int exitCode = -1;
        ProgramDriver pgd = new ProgramDriver();
        try {
            pgd.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            pgd.addClass("reduce_side_join", ReduceSideJoin.class, "reduce_side_join");
            exitCode = pgd.run(argv);
        }
        catch(Throwable e){
            e.printStackTrace();
        }

        System.exit(exitCode);
    }
}
