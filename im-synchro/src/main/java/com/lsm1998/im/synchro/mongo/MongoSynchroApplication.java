package com.lsm1998.im.synchro.mongo;

import com.ververica.cdc.connectors.mongodb.source.MongoDBSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MongoSynchroApplication
{
    /**
     * <a href="https://ververica.github.io/flink-cdc-connectors/master">flink cdc 文档</a>
     */

    public static void main(String[] args) throws Exception
    {
        MongoDBSource<String> mongoSource = MongoDBSource.<String>builder()
                .hosts("120.79.132.241:27017")
                .databaseList("im") // set captured database
                .collectionList("im.message") // set captured table
                .username("admin")
                .password("mongoyyds123")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint
        env.enableCheckpointing(3000);
        // set the source parallelism to 2
        env.fromSource(mongoSource, WatermarkStrategy.noWatermarks(), "MongoDBIncrementalSource")
                .setParallelism(2)
                .print()
                .setParallelism(1);

        env.execute("Print MongoDB Snapshot + Change Stream");
    }
}
