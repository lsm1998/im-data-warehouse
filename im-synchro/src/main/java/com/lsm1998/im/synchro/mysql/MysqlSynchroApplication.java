package com.lsm1998.im.synchro.mysql;

import com.lsm1998.im.synchro.serialization.Deserialization;
import com.lsm1998.im.synchro.sink.MessageSink;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class MysqlSynchroApplication
{
    /**
     * <a href="https://ververica.github.io/flink-cdc-connectors/master">flink cdc 文档</a>
     */

    public static void main(String[] args) throws Exception
    {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("120.79.132.241")
                .serverTimeZone("Asia/Shanghai")
                .port(3306)
                .databaseList("cdc-test") // set captured database
                .tableList("cdc-test.users") // set captured table
                .username("root")
                .password("mysqlyyds123")
                .startupOptions(StartupOptions.initial()) // 全量同步
                .deserializer(new Deserialization()) // converts SourceRecord to JSON String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(3000);

        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(1).
                addSink(new MessageSink());

        env.execute("CDC MySQL Job");
    }
}
