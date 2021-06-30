package processfunction;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;

/**
 * 核酸检测，确诊数超过100例病例，并在10秒内持续保持在100例以上，则输出城市信息，定义为高风险地区
 * 备注：为调试方便，此处时间设置较短，并不符合现实需求。
 */
public class ProcessFunctionExamples {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //模拟数据源：地区，核酸检测确诊人数，当前时间
        DataStream<Tuple3<String, Integer, Long>> dataStream = env.addSource(new SourceFunction<Tuple3<String, Integer, Long>>() {
            @Override
            public void run(SourceFunction.SourceContext<Tuple3<String, Integer, Long>> sourceContext) throws Exception {
                while (true) {
                    if (System.currentTimeMillis() % 2 == 0) {
                        // 治愈出院2例
                        sourceContext.collect(new Tuple3<String, Integer, Long>("SZ", -2, System.currentTimeMillis()));
                    } else {
                        // 确诊10例
                        sourceContext.collect(new Tuple3<String, Integer, Long>("SZ", 10, System.currentTimeMillis()));
                    }
                    Thread.sleep(100);
                }
            }

            @Override
            public void cancel() {
            }
        });

        //TODO 调用自定义的ProcessFunction处理函数
        dataStream.keyBy(new KeySelector<Tuple3<String, Integer, Long>, Object>() {
            @Override
            public Object getKey(Tuple3<String, Integer, Long> tp) throws Exception {
                return tp.f0;
            }
        }).process(new ConfirmIncreaseFunctoin()).print();

        env.execute("Nucleic Acid Test");//核酸检测
    }

    static class ConfirmIncreaseFunctoin extends ProcessFunction<Tuple3<String, Integer, Long>,String>{
        private final int threshold=100;
        private ValueState<Long> currentTime;
        private MapState<String,Integer> areaTime;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            currentTime=getRuntimeContext().getState(new ValueStateDescriptor<Long>(
                    "time"
                    , TypeInformation.of(new TypeHint<Long>() {})));
            areaTime=getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                    "MapStateDesc"
                    ,String.class
                    ,Integer.class
            ));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            // 10秒之后，输出超过100例的城市
            Iterator<Map.Entry<String, Integer>> iterator = areaTime.iterator();
            while (iterator.hasNext()){
                Map.Entry<String, Integer> map = iterator.next();
                if(map.getValue()>threshold){
                    out.collect(map.getKey());
                    areaTime.remove(map.getKey());
                }
            }
            currentTime.clear();
        }

        @Override
        public void processElement(Tuple3<String, Integer, Long> tp, Context context, Collector<String> collector) throws Exception {
            // 城市
            String city = tp.f0;
            // 确诊数量
            Integer num = tp.f1;

            // 不存在计时器
            if (areaTime.contains(city)) {
                Integer times = areaTime.get(city);
                Integer currentSumTimes = (times + num) > 0 ? (times + num) : 0;
                if (currentSumTimes >= threshold) {
                    // 超过100例，设置计时器，以当前processingtime+10000毫秒进行设置
                    long timerTs = context.timerService().currentProcessingTime() + 10000;
                    context.timerService().registerProcessingTimeTimer(timerTs);
                    currentTime.update(timerTs);
                } else {
                    // 小于100例
                    Long value = currentTime.value();
                    if(value !=null) {
                        context.timerService().deleteProcessingTimeTimer(currentTime.value());
                        areaTime.remove(city);
                        currentTime.clear();
                    }
                }
                areaTime.put(city, currentSumTimes);
            }else{
                areaTime.put(city, num > 0 ? num : 0);
            }
        }
    }
}
