package com.lsm1998.im.synchro.serialization;

import com.alibaba.fastjson2.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class Deserialization implements DebeziumDeserializationSchema<String>
{
    private JSONObject parseStruct(Struct value, String field)
    {
        Struct struct = value.getStruct(field);
        JSONObject result = new JSONObject();
        if (struct == null)
        {
            return result;
        }
        Schema schema = struct.schema();
        for (Field field1 : schema.fields())
        {
            result.put(field1.name(), struct.get(field1));
        }
        return result;
    }

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception
    {
        JSONObject result = new JSONObject();
        String[] split = sourceRecord.topic().split("\\.");
        result.put("db", split[1]);
        result.put("table", split[2]);
        Struct value = (Struct) sourceRecord.value();
        String[] fieldArray = {"after", "before", "source"};
        for (String field : fieldArray)
        {
            result.put(field, parseStruct(value, field));
        }
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        result.put("op", operation);
        collector.collect(result.toString());
    }

    @Override
    public TypeInformation<String> getProducedType()
    {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
