package com.ercai.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 读取离线txt，批处理统计单词
 * @author ercai caishiqi0125@163.com
 * @date 2021/5/17 - 17:23
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        //创建批处理执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //读取数据
        DataSource<String> input = env.readTextFile("src/main/resources/batchworkcount.txt");

        //统计数据
        DataSet<Tuple2<String, Integer>> res = input.flatMap(new MyflatMap()).groupBy(0).sum(1);

        //输出打印
        res.print();

    }
    private static class MyflatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
