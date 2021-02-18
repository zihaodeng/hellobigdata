package keyedstate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * ValueState<T> 保存一个值
 */
public class ValueStateExamples {
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

        // 计算
        dataStream.keyBy(0)
                .flatMap(new RichFlatMapFunction<Tuple3<String, Integer, Long>, Tuple2<String, Integer>>() {
                    private ValueState<Tuple2<String,Integer>> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<Tuple2<String, Integer>> valueStateDescriptor =
                                new ValueStateDescriptor<Tuple2<String, Integer>>(
                                        "ValueStateDesc"
                                        , TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));

                        // keyedstate设置state超时
                        StateTtlConfig ttlConfig = StateTtlConfig
                                .newBuilder(Time.seconds(60))//当前时间 - 上一次使用时间 >=60秒 则超时
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)//OnCreateAndWrite创建和写入  OnReadAndWrite读取和写入
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)//NeverReturnExpired超时元素绝不返回 ReturnExpiredIfNotCleanedUp数据没被删除可以返回
                                .build();

                        valueStateDescriptor.enableTimeToLive(ttlConfig);
                        valueState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void flatMap(Tuple3<String, Integer, Long> tupleInput, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        Tuple2<String, Integer> value = valueState.value();
                        if (value == null)
                            value=Tuple2.of(tupleInput.f0,tupleInput.f1);
                        else {
                            value.f0 = tupleInput.f0;
                            value.f1 += tupleInput.f1;
                        }
                        valueState.update(value);
                        collector.collect(value);
                        //valueState.clear();
                    }
                })
                .print();

        env.execute("zmboosum");
    }
}
