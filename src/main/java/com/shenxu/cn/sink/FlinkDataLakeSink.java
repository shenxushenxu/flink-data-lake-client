package com.shenxu.cn.sink;

import com.shenxu.cn.client.DataLakeClient;
import com.shenxu.cn.entity.LineData;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;

public class FlinkDataLakeSink implements Serializable {


    private int partitionNum;
    private String majorKey;
    private String dataLakeIp;
    private int dataLakePort;
    private String tableName;
    public FlinkDataLakeSink(String tableName, String dataLakeIp, int dataLakePort) throws IOException {

        this.tableName = tableName;
        this.partitionNum = partitionNum;
        this.majorKey = majorKey;
        this.dataLakeIp = dataLakeIp;
        this.dataLakePort = dataLakePort;
    }


    private int count = 50000;
    private long time = 30 * 1000;
    public void setCount(int count){
        this.count = count;
    }
    public void setTime(long time){
        this.time = time;
    }


    public void sink(DataStream<LineData> dataDataStream) throws Exception {


        dataDataStream.transform(tableName,
                TypeInformation.of(Void.class),
        new DataLakeSink(tableName, dataLakeIp, dataLakePort, count, time)
        );




//        dataDataStream.partitionCustom(new Partitioner<Integer>() {
//                    @Override
//                    public int partition(Integer key, int numPartitions) {
//                        return key % numPartitions;
//                    }
//                }, new KeySelector<LineData, Integer>() {
//                    @Override
//                    public Integer getKey(LineData value) throws Exception {
//                        return Math.abs(value.getString(majorKey).hashCode());
//                    }
//                }).keyBy(new KeySelector<LineData, Integer>() {
//                    @Override
//                    public Integer getKey(LineData value) throws Exception {
//                        return Math.abs(value.getString(majorKey).hashCode()) % partitionNum;
//                    }
//                }).process(new DataLakeSave(tableName, dataLakeIp, dataLakePort,partitionNum, count, time))
//                .uid(tableName)
//                .name(tableName)
//                .setParallelism(partitionNum)
//                .addSink(new SinkFunction<String>() {
//                    @Override
//                    public void invoke(String value, Context context) throws Exception {
//                        System.out.println(value);
//                    }
//                }).setParallelism(1);



    }


}
