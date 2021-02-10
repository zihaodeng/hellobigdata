package func;

import model.Covid19Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class Covid19MapFunc implements MapFunction<Covid19Event, Tuple3<String, Integer, Long>> {
    /**
     * Covid19Event -> Tuple3
     * @param event
     * @return
     * @throws Exception
     */
    @Override
    public Tuple3<String, Integer, Long> map(Covid19Event event) throws Exception {
        String cityCode = event.getCityCode();
        Integer count = event.getCount();
        Long timestamp = event.getTimestamp();
        return  new Tuple3<>(cityCode, count, timestamp);
    }
}
