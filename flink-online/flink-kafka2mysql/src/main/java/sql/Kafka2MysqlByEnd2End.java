package sql;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import udf.ExchangeGoodsName;

import java.util.concurrent.TimeUnit;

public class Kafka2MysqlByEnd2End {
    public static void main(String[] args) throws Exception {
        // Kafka source
        String sourceSQL="CREATE TABLE order_source (\n" +
                "   payTime VARCHAR,\n" +
                "   rt as TO_TIMESTAMP(payTime),\n" +
                "   orderId BIGINT,\n" +
                "   goodsId INT,\n" +
                "   userId INT,\n" +
                "   amount DECIMAL(23,10),\n" +
                "   address VARCHAR,\n" +
                "   WATERMARK FOR rt as rt - INTERVAL '2' SECOND\n" +
                " ) WITH (\n" +
                "   'connector' = 'kafka-0.11',\n" +
                "   'topic'='order-log',\n" +
                "   'properties.bootstrap.servers'='kafka:9092',\n" +
                "   'format' = 'json',\n" +
                "   'scan.startup.mode' = 'latest-offset'\n" +
                ")";

        //Mysql sink
        String sinkSQL="CREATE TABLE order_sink (\n" +
                "   goodsId BIGINT,\n" +
                "   goodsName VARCHAR,\n" +
                "   amount DECIMAL(23,10),\n" +
                "   rowtime TIMESTAMP(3),\n" +
                "   PRIMARY KEY (goodsId) NOT ENFORCED\n" +
                " ) WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://mysql:3306/flinkdb?characterEncoding=utf-8&useSSL=false',\n" +
                "   'table-name' = 'good_sale',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456',\n" +
                "   'sink.buffer-flush.max-rows' = '1',\n" +
                "   'sink.buffer-flush.interval' = '1s'\n" +
                ")";

//        sinkSQL="CREATE TABLE order_sink (\n" +
//                "   goodsId INT,\n" +
//                "   goodsName VARCHAR,\n" +
//                "   amount DECIMAL(23,10),\n" +
//                "   rowtime TIMESTAMP(3)\n" +
//                //"   PRIMARY KEY (goodsId) NOT ENFORCED)\n" +
//                " ) WITH (\n" +
//                "   'connector' = 'print'" +
//                ")";

        // 创建执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        //TableEnvironment tEnv = TableEnvironment.create(settings);
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        //sEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(1, TimeUnit.SECONDS)));
        //sEnv.enableCheckpointing(1000);
        //sEnv.setStateBackend(new FsStateBackend("file:///tmp/chkdir",false));

        StreamTableEnvironment tEnv= StreamTableEnvironment.create(sEnv,settings);

        Configuration configuration = tEnv.getConfig().getConfiguration();
        //设置并行度为1
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 1);

        //注册souuce
        tEnv.executeSql(sourceSQL);
        //注册sink
        tEnv.executeSql(sinkSQL);

        //UDF 在作业中定义UDF
        tEnv.createFunction("exchangeGoods", ExchangeGoodsName.class);

        String strSQL=" SELECT " +
                "   goodsId," +
                "   exchangeGoods(goodsId) as goodsName, " +
                "   sum(amount) as amount, " +
                "   tumble_start(rt, interval '5' seconds) as rowtime " +
                " FROM order_source " +
                " GROUP BY tumble(rt, interval '5' seconds),goodsId";

        //查询数据 插入数据
        tEnv.sqlQuery(strSQL).executeInsert("order_sink");

        //执行作业
        tEnv.execute("统计各商品营业额");

    }
}
