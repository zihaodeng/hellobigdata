package sql;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * Kafka生产的数据：
 *      {"country_id":"1","country_msg":"hello flink!"}
 * MySql建表语句:
 *      create table country(country_id bigint,country_msg varchar(100));
 */
public class Kafka2Mysql {
    public static void main(String[] args) throws Exception {
        // Kafka source
        String sourceSQL="CREATE TABLE demo_source (country_id BIGINT,country_msg STRING)\n" +
                " WITH (\n" +
                " 'connector' = 'kafka-0.11',\n" +
                " 'topic'='country-log',\n" +
                " 'properties.bootstrap.servers'='localhost:9092',\n" +
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'latest-offset'\n" +
                ")";

        //Mysql sink
        String sinkSQL="CREATE TABLE demo_sink (country_id BIGINT,country_msg STRING)\n" +
                " WITH (" +
                "  'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://localhost:3306/flinkdb?characterEncoding=utf-8&useSSL=false',\n" +
                "   'table-name' = 'country_log',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456',\n" +
                "   'sink.buffer-flush.max-rows' = '1',\n" +
                "   'sink.buffer-flush.interval' = '1s'\n" +
                ")";

        // 创建执行环境
        EnvironmentSettings settings=EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        //TableEnvironment tEnv = TableEnvironment.create(settings);
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        sEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(1, TimeUnit.SECONDS)));
        //sEnv.enableCheckpointing(1000);
        //sEnv.setStateBackend(new FsStateBackend("file:///tmp/chkdir",false));

        StreamTableEnvironment tEnv= StreamTableEnvironment.create(sEnv,settings);

        //注册souuce
        tEnv.executeSql(sourceSQL);
        //注册sink
        tEnv.executeSql(sinkSQL);

        //数据提取
        Table sourceTable=tEnv.from("demo_source");

        //发送数据
        sourceTable.executeInsert("demo_sink");
        //执行作业
        tEnv.execute("Hello Flink");
    }
}
