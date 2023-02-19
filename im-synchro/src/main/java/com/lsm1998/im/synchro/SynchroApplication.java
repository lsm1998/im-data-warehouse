package com.lsm1998.im.synchro;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SynchroApplication
{
    /**
     * <a href="https://ververica.github.io/flink-cdc-connectors/master">flink cdc 文档</a>
     */

    public static void main(String[] args) throws Exception
    {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("120.79.132.241")
                .port(3306)
                .databaseList("cdc-test") // set captured database
                .tableList("cdc-test.op_log,im-admin.users") // set captured table
                .username("root")
                .password("mysqlyyds123")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(3000);

        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // set 4 parallel source tasks
                .setParallelism(4)
                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
