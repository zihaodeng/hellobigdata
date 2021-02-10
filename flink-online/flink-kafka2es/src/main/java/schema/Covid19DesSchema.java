package schema;

import model.Covid19Event;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Covid19DesSchema implements DeserializationSchema<Covid19Event> {

    private final Class<Covid19Event> clazz;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public Covid19DesSchema() {
        this.clazz=Covid19Event.class;
    }

    /**
     * 每来一条数据进入此方法，进行反序列化
     * @param bytes
     * @return
     * @throws IOException
     */
    @Override
    public Covid19Event deserialize(byte[] bytes) throws IOException {
        //ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        // json字符串
        //String message = new String(bytes);

        JsonNode jsonNode = objectMapper.readTree(bytes);
        String city_code=jsonNode.get("city_code").asText();
        Integer count=jsonNode.get("count").asInt();
        Long timestamp = jsonNode.get("timestamp").asLong();
        return new Covid19Event(city_code, count, timestamp);
    }

    @Override
    public boolean isEndOfStream(Covid19Event covid19Event) {
        return false;
    }

    @Override
    public TypeInformation<Covid19Event> getProducedType() {
        return TypeExtractor.getForClass(clazz);
    }
}

/*
public class Covid19DesSchema implements KafkaDeserializationSchema<Covid19Event> {

    @Override
    public boolean isEndOfStream(Covid19Event covid19Event) {
        return false;
    }

    @Override
    public Covid19Event deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        consumerRecord.topic();
        consumerRecord.partition();
        consumerRecord.offset();
        consumerRecord.timestamp();
        consumerRecord.timestampType();
        consumerRecord.checksum();
        consumerRecord.serializedKeySize();
        consumerRecord.serializedValueSize();
        new  String(consumerRecord.key(), "UTF8");
        new  String(consumerRecord.value(), "UTF8");
        return new Covid19Event();
    }

    @Override
    public TypeInformation<Covid19Event> getProducedType() {
        return null;
    }
}
 */
