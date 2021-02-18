package OperatorState;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * OperatorState中，UnionListState相对于ListState更加灵活
 * ListState 恢复后策略是：均匀的分配给每个subtask
 * UnionListState 恢复后策略是：将所有状态信息合并后，分配给每个subtask，这意味着每个subtask都将获取到全量的信息
 */
public class UnionListStateExamples {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度设置
        //env.setParallelism(4);
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
        }).addSink(new Covid19Sink(10)).setParallelism(2);

        env.execute("zmboosum");
    }

    /**
     * 自定义source
     */
    private static class Covid19Source extends RichSourceFunction<Tuple3<String, Integer, Long>> implements CheckpointedFunction{
        private int subtaskIndex = 0;
        private Long offset = 0L;
        private ListState<Tuple2<Integer,Long>> unionListState;
        private volatile boolean isRunning = true;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            //保存最新的subtask的offset
            //context.getCheckpointId();
            unionListState.clear();
            unionListState.add(Tuple2.of(subtaskIndex, offset));
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            int currentSubtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

            ListStateDescriptor<Tuple2<Integer,Long>> listStateDescriptor =
                    new ListStateDescriptor<Tuple2<Integer,Long>>(
                            "UnionListStateDesc"
                            , TypeInformation.of(new TypeHint<Tuple2<Integer,Long>>() {})
                    );

            unionListState = context.getOperatorStateStore().getUnionListState(listStateDescriptor);

            //获取最新的
            if(context.isRestored()){
                //unionListState需要用户根据自己的业务，过滤其中需要的State
                for (Tuple2<Integer,Long> element : unionListState.get()) {
                    //仅供参考
                    if (element.f0 == currentSubtaskIndex) {
                        offset = element.f1;
                    }
                }
            }
        }

        @Override
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

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    /**
     * 自定义sink
     */
    private static class Covid19Sink extends RichSinkFunction<Tuple2<String, Integer>> implements CheckpointedFunction{
        /** 多少条数据一次sink */
        private final int threshold;

        private int subtaskIndex = 0;
        /** buffer缓冲state */
        private List<Tuple3<Integer,String,Integer>> bufferedElements;
        /** cp state */
        private ListState<Tuple3<Integer,String, Integer>> checkpointedState;

        public Covid19Sink(int threshold){
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<Tuple3<Integer,String,Integer>>();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        }
        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            checkpointedState.clear();
            for (Tuple3<Integer,String,Integer> element : bufferedElements) {
                checkpointedState.add(element);
            }
        }
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            int currentSubtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

            ListStateDescriptor<Tuple3<Integer,String, Integer>> listStateDescriptor =
                    new ListStateDescriptor<Tuple3<Integer,String, Integer>>(
                            "ListStateDesc"
                            , TypeInformation.of(new TypeHint<Tuple3<Integer,String, Integer>>() {})
                    );

            checkpointedState = context.getOperatorStateStore().getUnionListState(listStateDescriptor);

            //故障恢复
            if (context.isRestored()) {
                //unionListState恢复后，将获取到所有状态的信息；若有需要，用户要根据自己的业务，过滤其中需要的State
                for (Tuple3<Integer,String, Integer> element : checkpointedState.get()) {
                    if (element.f0 == currentSubtaskIndex) {
                        bufferedElements.add(element);
                    }
                }
            }
        }
        @Override
        public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
            bufferedElements.add(Tuple3.of(subtaskIndex, value.f0, value.f1));
            if (bufferedElements.size() == threshold) {
                //每满threshold数量send一次
                for (Tuple3<Integer,String,Integer> element: bufferedElements) {
                    // send it to the sink
                    System.out.println(element);
                }
                bufferedElements.clear();
            }
        }
    }

}
