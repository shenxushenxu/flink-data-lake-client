package com.datalake.cn.source;

import com.datalake.cn.entity.DataLakeStreamData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;

public class FlinkDataLakeReadSource implements Serializable {


    public static DataStream<DataLakeStreamData> source(
            StreamExecutionEnvironment env,
            String dataLakeIp,
            int dataLakePort,
            String tableName,
            int readCount) throws Exception {

        FlinkStreamRead flinkStreamRead = new FlinkStreamRead(dataLakeIp, dataLakePort, tableName, readCount);

        SingleOutputStreamOperator<DataLakeStreamData> dataLakeStreamDataDataStreamSource = env.addSource(flinkStreamRead).name(tableName).uid(tableName).setParallelism(1)
                .keyBy(new KeySelector<DataLakeStreamData, String>() {
                    @Override
                    public String getKey(DataLakeStreamData value) throws Exception {
                        return value.getMajorValue();
                    }
                }).map(new MapFunction<DataLakeStreamData, DataLakeStreamData>() {
                    @Override
                    public DataLakeStreamData map(DataLakeStreamData value) throws Exception {
                        return value;
                    }
                }).name("Repartitioning");

        return dataLakeStreamDataDataStreamSource;
    }


}
