package func;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.util.Collector;

public class Covid19LevelFlatMapFunc extends RichFlatMapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>> {

    // 城市code -> 确诊数量
    private MapState<String,Integer> mapState;
    // 城市code 确诊数量 风险等级
    private ListState<Tuple3<String,Integer,Integer>> listState;

    @Override
    public void open(Configuration parameters) throws Exception {
        //super.open(parameters);
        MapStateDescriptor<String, Integer> descriptor =
                new MapStateDescriptor<String, Integer>(
                        "Covid19Level",
                        String.class,
                        Integer.class
                );
        mapState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<String, Integer> stringIntegerTuple2, Collector<Tuple2<String,Integer>> collector) throws Exception {
        String cityCode = stringIntegerTuple2.f0;
        Integer count = stringIntegerTuple2.f1;

        Integer hisCount = mapState.contains(cityCode) ? mapState.get(cityCode) : 0;
        Integer nowCount = hisCount + count;
        Integer level;
        mapState.put(cityCode,nowCount);
        if(nowCount.compareTo(50)>0){
            //高风险
            level=2;
        }
        else if(nowCount.compareTo(10)>0 && nowCount.compareTo(50)<=0){
            //中风险
            level=1;
        }else{
            //低风险
            level=0;
        }
        //
        collector.collect(Tuple2.of(cityCode,level));
        //collector.collect(String.format("{\"city_code\":\"%s\",\"level\":%d}",cityCode,level));
    }

}
