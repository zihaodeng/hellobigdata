package keyedstate;


import func.Covid19AggFunc;
import func.Covid19ESSink;
import func.Covid19MapFunc;
import model.Covid19Event;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.http.HttpHost;
import schema.Covid19DesSchema;
import watermark.Covid19Watermark;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * flink 1.11.3
 * 疫情低、中、高风险地区 （假定疫情时刻在变化）
 * 一天以内
 * 1-10 低风险 0
 * 11-50 中风险 1
 * >=51 高风险 2
 * GD 0
 * HB 2
 * BJ 1
 * kafka {"city_code":"SZ","count":"6"}
 * ES {"city_code":"SZ","level":"0"}
 */
public class Kafka2ESByEnd2End {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度设置
        // env.setParallelism(3);
        // 获取默认并行度 本地->CPU核数 集群->默认配置
        //int pm = env.getParallelism();
        // 设置Checkpoint 每个60*1000ms一次cp
        env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE);
        // 10分钟内 重启三次 每次间隔10秒 超过则job失败
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.of(10,TimeUnit.MINUTES), org.apache.flink.api.common.time.Time.of(10,TimeUnit.SECONDS)));
        // 设置statebackend 暂用Memory
        env.setStateBackend(new MemoryStateBackend(true));
        // 设置EventTime为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // source
        Properties properties = new Properties();
        // 集群配置多个kafka地址properties.setProperty("bootstrap.servers", "kafka120:9092,kafka121:9092");
        properties.setProperty("bootstrap.servers", "kafka:9092");
        properties.setProperty("group.id", "grouplevel");
//        DataStream<String> dataStream = env
//                .addSource(new FlinkKafkaConsumer011<String>("covid19count-log", new SimpleStringSchema(), properties));
        DataStream<Covid19Event> dataStream = env
                .addSource(new FlinkKafkaConsumer011<Covid19Event>("covid19count-log", new Covid19DesSchema(), properties));

        /* 模拟数据源
        List<ObjectNode> list=new ArrayList();
        ObjectNode objectNode=new ObjectNode(new JsonNodeFactory(false));
        objectNode.put("city_code","SZ");
        objectNode.put("count", 1);
        list.add(objectNode);
        DataStream<ObjectNode> dataStream = env.fromCollection(list);
        */
        /* 模拟数据源
        DataStream<SourceEvent> dataStream = env.addSource(new SourceFunction<SourceEvent>() {
            @Override
            public void run(SourceContext<SourceEvent> sourceContext) throws Exception {
                int index = 1;
                while (true) {
//                    ObjectNode objectNode=new ObjectNode(new JsonNodeFactory(false));
//                    objectNode.put("city_code","SZ");
//                    objectNode.put("count", 1);
//                    sourceContext.collect(objectNode);
                    if(System.currentTimeMillis() %2 ==0) {
                        sourceContext.collect(new SourceEvent("SZ", 1, System.currentTimeMillis()));
                    }else{
                        sourceContext.collect(new SourceEvent("BJ", 10, System.currentTimeMillis()));
                    }

                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {}
        });
        */

        // sink
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("elasticsearch", 9200, "http"));
        // 集群add多个地址
        //httpHosts.add(new HttpHost("10.2.3.1", 9200, "http"));
        // use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<Tuple3<String,Integer,Long>> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new Covid19ESSink()
        );

        // 算子计算
        dataStream.assignTimestampsAndWatermarks(new Covid19Watermark())
                // 流的合并与拆分
                //.union()//合并n个流
                //.connect()//合并2个流 输出1个流 coMap coFlatMap
                //.split()//union的逆操作 将输入流拆成多个输出流 Select
                //计算
                //.map()//1转1的基本转换
                //.filter()//过滤
                //.flatMap()//1转n 前面2个的泛化 能够实现前面2个转换 比较通用
                .map(new Covid19MapFunc())
                // 分区策略 datastream
                //.shuffle()//随机
                //.rebalance()//轮流所有
                //.rescale()//轮流部分
                //.global()//所有事件发往下游算子的第一个并行任务 注意性能
                //.partitionCustom()//自定义分区策略
                // 分区 keyedstream
                //.keyBy()//1个或多个key
                .keyBy(0)//可以访问keyedstate
                //.sum()
                //.min()
                //.minBy()
                //.max()
                //.maxBy()
                //.reduce()//上面聚合方法的泛化 将事件进行合并/组合
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))//10秒钟的窗口，滑动间隔是5秒 滑动窗口可能触发两次计算
                .aggregate(new Covid19AggFunc())
                //.flatMap(new Covid19LevelFlatMapFunc()).print();
                .addSink(esSinkBuilder.build());

                // 调试sink -> print()

        env.execute("Covid19StaticLevel");
    }

}


