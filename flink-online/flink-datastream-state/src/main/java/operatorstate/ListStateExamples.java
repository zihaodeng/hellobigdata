package operatorstate;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * OperatorState中，ListState常用在source和sink端，保存偏移量offset等信息
 * ListState 恢复后策略是：均匀的分配给每个subtask
 */
public class ListStateExamples {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度设置
        env.setParallelism(4);
        // 获取默认并行度 本地->CPU核数 集群->默认配置
        //int pm = env.getParallelism();
        // 设置Checkpoint 每个60*1000ms一次cp
        env.enableCheckpointing(10 * 1000, CheckpointingMode.EXACTLY_ONCE);
        // 10分钟内 重启三次 每次间隔10秒 超过则job失败
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.of(10, TimeUnit.MINUTES), org.apache.flink.api.common.time.Time.of(10,TimeUnit.SECONDS)));
        // 设置statebackend 暂用Memory
        env.setStateBackend(new MemoryStateBackend(true));
        // 设置EventTime为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // source 模拟数据源 自定义
        DataStream<Tuple3<String,Integer,Long>> dataStream = env.addSource(new Covid19Source());

        // 计算
        dataStream.map(new MapFunction<Tuple3<String, Integer, Long>, Tuple2<String,Integer>>() {
            public Tuple2<String, Integer> map(Tuple3<String, Integer, Long> tupleInput) throws Exception {
                return Tuple2.of(tupleInput.f0, tupleInput.f1);
            }
        }).addSink(new Covid19Sink(10));

        env.execute("zmboosum");
    }

    /**
     * 自定义source
     */
    private static class Covid19Source implements SourceFunction<Tuple3<String, Integer, Long>>, CheckpointedFunction {

        /**  current offset for exactly once semantics */
        private Long offset = 0L;
        /** flag for job cancellation */
        private volatile boolean isRunning = true;
        /** Our state object. */
        private ListState<Long> listState;

        /**
         * checkpoint
         * @param functionSnapshotContext
         * @throws Exception
         */
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            //保存最新的offset
            listState.clear();
            listState.add(offset);
        }

        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Long> listStateDescriptor =
                    new ListStateDescriptor<Long>(
                            "ListStateDesc"
                            , Long.class
                    );

            listState = context.getOperatorStateStore().getListState(listStateDescriptor);

            //获取最新的offset
            for (Long l : listState.get()) {
                offset = l;
            }
        }

        public void run(SourceContext<Tuple3<String, Integer, Long>> sourceContext) throws Exception {
            Object checkpointLock = sourceContext.getCheckpointLock();
            while (isRunning) {
                // output and state update are atomic
                synchronized (checkpointLock) {
                    if(System.currentTimeMillis() %2 ==0) {
                        sourceContext.collect(new Tuple3<String,Integer,Long>("SZ", 1, System.currentTimeMillis()));
                    }else{
                        sourceContext.collect(new Tuple3<String, Integer, Long>("BJ", 10, System.currentTimeMillis()));
                    }

                    offset += 1;
                    Thread.sleep(1000);
                }
            }
        }

        public void cancel() {
            isRunning = false;
        }
    }

    /**
     * 自定义sink
     */
    private static class Covid19Sink implements SinkFunction<Tuple2<String,Integer>>,  CheckpointedFunction{
        /** 多少条数据一次sink */
        private final int threshold;
         /** buffer缓冲state */
        private List<Tuple2<String,Integer>> bufferedElements;
         /** cp state */
        private ListState<Tuple2<String, Integer>> checkpointedState;

        public Covid19Sink(int threshold){
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<Tuple2<String, Integer>>();
        }

        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Tuple2<String, Integer>> listStateDescriptor =
                    new ListStateDescriptor<Tuple2<String, Integer>>(
                            "ListStateDesc"
                            , TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {})
                    );

            checkpointedState = context.getOperatorStateStore().getListState(listStateDescriptor);

            //故障恢复
            if (context.isRestored()) {
                for (Tuple2<String, Integer> element : checkpointedState.get()) {
                    bufferedElements.add(element);
                }
            }
        }

        /**
         * checkpoint
         * @param functionSnapshotContext
         * @throws Exception
         */
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            checkpointedState.clear();
            for (Tuple2<String, Integer> element : bufferedElements) {
                checkpointedState.add(element);
            }
        }

        public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
            bufferedElements.add(value);
            if (bufferedElements.size() == threshold) {
                //每满threshold数量send一次
                for (Tuple2<String, Integer> element: bufferedElements) {
                    // send it to the sink
                    System.out.println(element);
                }
                bufferedElements.clear();
            }
        }
    }
}
