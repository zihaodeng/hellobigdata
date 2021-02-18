package OperatorState;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * BroadcastState用于将数据广播给下游所有算子
 * BroadcastStream可以与一个DataStream或KeyedStream使用connect方法连接到一起
 * connect后实现ProcessFunction方法：
 *      如果是DataStream则需要实现BroadcastProcessFunction方法；
 *      如果是KeyedStream则需要实现KeyedBroadcastProcessFunction方法。
 */
public class BroadcastStateExamples {

    // MapStateDescriptor 表述的 Broadcast State
    private static final MapStateDescriptor<String, Integer> descriptor = new MapStateDescriptor(
            "BroadcastStateConfig"
            ,String.class
            ,Integer.class
    );

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度设置
        env.setParallelism(4);
        // 获取默认并行度 本地->CPU核数 集群->默认配置
        //int pm = env.getParallelism();
        // 设置Checkpoint 每个60*1000ms一次cp
        env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE);
        // 10分钟内 重启三次 每次间隔10秒 超过则job失败
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.of(10, TimeUnit.MINUTES), org.apache.flink.api.common.time.Time.of(10,TimeUnit.SECONDS)));
        // 设置statebackend 暂用Memory
        env.setStateBackend(new MemoryStateBackend(true));
        // 设置EventTime为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // source 模拟数据源
        DataStream<Tuple3<String,Integer,Long>> dataStream = env.addSource(new SourceFunction<Tuple3<String, Integer, Long>>() {
            public void run(SourceContext<Tuple3<String, Integer, Long>> sourceContext) throws Exception {
                while (true) {
                    if(System.currentTimeMillis() %2 ==0) {
                        sourceContext.collect(new Tuple3<String,Integer,Long>("SZ", 1, System.currentTimeMillis()));
                    }else{
                        sourceContext.collect(new Tuple3<String, Integer, Long>("BJ", 10, System.currentTimeMillis()));
                    }

                    Thread.sleep(1000);
                }
            }

            public void cancel() {}
        });

        // 自定义数据源（模拟） 进行一个简单的map转换 再转换成广播流
        BroadcastStream<Tuple2<String,Integer>> broadcastStream = env.addSource(new SourceFunction<Tuple3<String, Integer, Long>>() {
            public void run(SourceContext<Tuple3<String, Integer, Long>> sourceContext) throws Exception {
                while (true) {
                    if(System.currentTimeMillis() %2 ==0) {
                        sourceContext.collect(new Tuple3<String,Integer,Long>("SZ", 0, System.currentTimeMillis()));
                    }else{
                        sourceContext.collect(new Tuple3<String, Integer, Long>("BJ", 1, System.currentTimeMillis()));
                    }

                    Thread.sleep(1000);
                }
            }

            public void cancel() {}
        }).map(new MapFunction<Tuple3<String, Integer, Long>, Tuple2<String,Integer>>() {
            public Tuple2<String, Integer> map(Tuple3<String, Integer, Long> tupleInput) throws Exception {
                return Tuple2.of(tupleInput.f0, tupleInput.f1);
            }
        }).broadcast(descriptor);

        dataStream.keyBy(new KeySelector<Tuple3<String, Integer, Long>,String>() {
            public String getKey(Tuple3<String, Integer, Long> tupleInput) throws Exception {
                return tupleInput.f0;
            }
        })
                .connect(broadcastStream)
                .process(new Covid19Broadcast())
                .print();

        env.execute("zmboosum");
    }

    /**
     * KeyedBroadcastProcessFunction参数：
     *      1,KeyStream中Key的类型
     *      2,业务流KeyedStream数据类型
     *      3,广播流BroadcastStream数据类型
     *      4,输出的数据类型
     */
    private static class Covid19Broadcast
            extends KeyedBroadcastProcessFunction<String, Tuple3<String, Integer, Long>, Tuple2<String, Integer>, Tuple3<Integer, Integer, Long>> {

        /**
         * 在每条正常的业务流数据(KeyedStream)进入的时候进行调用
         * @param tupleInput
         * @param readOnlyContext 只读的Broadcast State
         * @param collector
         * @throws Exception
         */
        @Override
        public void processElement(Tuple3<String, Integer, Long> tupleInput, ReadOnlyContext readOnlyContext, Collector<Tuple3<Integer, Integer, Long>> collector) throws Exception {
            ReadOnlyBroadcastState<String,Integer> broadcastState = readOnlyContext.getBroadcastState(descriptor);
            collector.collect(new Tuple3<Integer, Integer, Long>(broadcastState.get(tupleInput.f0), tupleInput.f1, tupleInput.f2));
        }

        /**
         * 在每条广播流(BroadcastStream)进入的时候进行调用
         * 可用来更新Boardcast State
         * @param tupleInput
         * @param context
         * @param collector
         * @throws Exception
         */
        @Override
        public void processBroadcastElement(Tuple2<String, Integer> tupleInput, Context context, Collector<Tuple3<Integer, Integer, Long>> collector) throws Exception {
            if(tupleInput.f0.equals("BJ")) {
                context.getBroadcastState(descriptor).put(tupleInput.f0, 8);
            }else{
                context.getBroadcastState(descriptor).put(tupleInput.f0, tupleInput.f1);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<Integer, Integer, Long>> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            //可以用来清除State，避免State一直增长
        }
    }
}
