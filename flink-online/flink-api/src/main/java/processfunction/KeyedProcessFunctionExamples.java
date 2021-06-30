package processfunction;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * 核酸检测，10秒内连续确诊超过3例病例，则输出城市信息，定义为高风险地区
 * 备注：为调试方便，此处时间设置较短，并不符合现实需求。
 */
public class KeyedProcessFunctionExamples {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //模拟数据源：地区，核酸检测确诊人数，当前时间
        DataStream<Tuple3<String, Integer, Long>> dataStream = env.addSource(new SourceFunction<Tuple3<String, Integer, Long>>() {
            @Override
            public void run(SourceFunction.SourceContext<Tuple3<String, Integer, Long>> sourceContext) throws Exception {
                while (true) {
                    if (System.currentTimeMillis() % 2 == 0) {
                        sourceContext.collect(new Tuple3<String, Integer, Long>("SZ", 1, System.currentTimeMillis()));
                    } else {
                        sourceContext.collect(new Tuple3<String, Integer, Long>("BJ", 10, System.currentTimeMillis()));
                    }

                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });

        //TODO KeyBy分区后，调用自定义的KeyedProcessFunction处理函数
        dataStream.keyBy(0)
                .process(new ConfirmIncreaseFunctoin()).print();

        env.execute("Nucleic Acid Test");//核酸检测
    }

    private static class ConfirmIncreaseFunctoin extends KeyedProcessFunction<Tuple,Tuple3<String, Integer, Long>,String> {
        private ValueState<Long> currentTime;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            currentTime=getRuntimeContext().getState(new ValueStateDescriptor<Long>(
                    "time"
                    , TypeInformation.of(new TypeHint<Long>() {})));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            Tuple currentKey = ctx.getCurrentKey();
            out.collect(currentKey.toString());
            currentTime.clear();
        }

        @Override
        public void processElement(Tuple3<String, Integer, Long> tp, Context context, Collector<String> collector) throws Exception {
            String area = tp.f0;
            Integer num = tp.f1;

            Long value = currentTime.value();
            // 不存在计时器
            if(value == null){
                if(num>=3){
                    long timerTs = context.timerService().currentProcessingTime()+10000;
                    // 设置计时器，以当前processingtime+10000毫秒进行设置
                    context.timerService().registerProcessingTimeTimer(timerTs);
                    currentTime.update(timerTs);
                }
            }
            else {
                // 已经存在计时器
                if(num<3) {
                    // 删除当前计时器
                    context.timerService().deleteProcessingTimeTimer(value);
                    currentTime.clear();
                }
            }
        }
    }
}
