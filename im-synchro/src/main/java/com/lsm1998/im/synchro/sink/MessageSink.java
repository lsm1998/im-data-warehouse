package com.lsm1998.im.synchro.sink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class MessageSink implements SinkFunction<String>
{
    @Override
    public void invoke(String value, Context context) throws Exception
    {
        System.out.println(value);
    }
}
