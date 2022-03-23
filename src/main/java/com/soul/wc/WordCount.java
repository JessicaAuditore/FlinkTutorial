package com.soul.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class WordCount {

    public static void main(String[] args) throws Exception {
//        batchProcessing();
        streamingProcessing(args);
    }

    // 批处理 针对离线数据集
    static void batchProcessing() throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据
        String inputPath = "src/main/resources/hello.txt";
        DataSet<String> inputDataSet = env.readTextFile(inputPath);

        // 对数据集进行处理，按空格分词展开，转换成(word, 1)二元组进行统计
        DataSet<Tuple2<String, Integer>> resultSet = inputDataSet.flatMap(new MyFlatMapFunction())
                .groupBy(0).sum(1); // 按照第一个位置的word分组，将第二个位置上的数据求和

        resultSet.print();
    }

    // 流处理 针对实时数据集
    static void streamingProcessing(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        //env.setParallelism(10);

        // 从文件中读取数据
//        String inputPath = "src/main/resources/hello.txt";
//        DataStream<String> inputDataStream = env.readTextFile(inputPath);

        // 真正实现流式数据
        // 从socket文本流读取数据 nc -lk 7777
        // 用parameter tool工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        DataStream<String> inputDataStream = env.socketTextStream(parameterTool.get("host"), parameterTool.getInt("port"));

        // 基于数据流进行转换计算
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new MyFlatMapFunction())
                .keyBy(0).sum(1).setParallelism(2).slotSharingGroup("red");

        resultStream.print().setParallelism(1);

        // 执行任务
        env.execute();

        /*
         * 3> (hello,7)  '3>'：并行执行的线程编号(分区编号)
         * */
    }

    static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
            // 按空格分词
            String[] words = s.split(" ");
            // 遍历所有word，包成二元组输出
            for (String word : words) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
