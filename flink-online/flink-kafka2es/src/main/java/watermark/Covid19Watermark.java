package watermark;

import model.Covid19Event;
import org.apache.flink.api.common.eventtime.*;
public class Covid19Watermark implements WatermarkStrategy<Covid19Event>{

    @Override
    public WatermarkGenerator<Covid19Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new Covid19WatermarkGenerator();
    }

    class Covid19WatermarkGenerator implements WatermarkGenerator<Covid19Event> {

        private final long delayTime = 3000;// 毫秒

        private long currentMaxTimestamp ;

        @Override
        public void onEvent(Covid19Event covid19Event, long eventTimestamp, WatermarkOutput watermarkOutput) {
            currentMaxTimestamp = Math.max(currentMaxTimestamp, covid19Event.getTimestamp());
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            watermarkOutput.emitWatermark(new Watermark(System.currentTimeMillis() - delayTime));
        }
    }
}


