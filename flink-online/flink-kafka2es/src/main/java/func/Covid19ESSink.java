package func;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.update.UpdateRequest;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Covid19ESSink implements ElasticsearchSinkFunction<Tuple3<String,Integer,Long>> {

    @Override
    public void process(Tuple3<String, Integer, Long> element, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
        requestIndexer.add(updateIndexRequest(element));
    }

      /* insert
                    public IndexRequest createIndexRequest(Tuple3<String,Integer,Long> element) {
                        Map<String, Object> json = new HashMap<>();
                        //json.put("data", String.format("{\"city_code\":\"%s\",\"level\":%d,\"timestamp\":%s}",element.f0,element.f1,element.f2));
                        json.put("city_code", element.f0);
                        json.put("level", element.f1);
                        json.put("timestamp", element.f2);
                        return Requests.indexRequest()
                                .index("covid19-index")
                                .type("covid19-type")
                                .id(element.f0)
                                .source(json);
                    }
                    */

    // upsert
    public UpdateRequest updateIndexRequest(Tuple3<String,Integer,Long> element) {
        Map<String, Object> map = new HashMap<>();
        //json.put("data", String.format("{\"city_code\":\"%s\",\"level\":%d,\"timestamp\":%s}",element.f0,element.f1,element.f2));
        map.put("city_code", element.f0);
        map.put("level", element.f1);
        map.put("timestamp", element.f2);

        UpdateRequest updateRequest=new UpdateRequest();
        updateRequest.docAsUpsert(true).retryOnConflict();
        return updateRequest
                .index("covid19-index")
                .type("covid19-type")
                .id(element.f0)
                .doc(map);
    }

}
