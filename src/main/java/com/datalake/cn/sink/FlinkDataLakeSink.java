package com.datalake.cn.sink;

import com.datalake.cn.entity.LineData;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class FlinkDataLakeSink implements Serializable {

    private String dataLakeIp;
    private int dataLakePort;
    private String tableName;
    private List<String> insertColumnName;
    public FlinkDataLakeSink(String tableName, String dataLakeIp, int dataLakePort, List<String> insertColumnName) throws IOException {

        this.tableName = tableName;
        this.dataLakeIp = dataLakeIp;
        this.dataLakePort = dataLakePort;
        this.insertColumnName = insertColumnName;
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
        new DataLakeSink(tableName, dataLakeIp, dataLakePort, insertColumnName, count, time)
        );

    }


}
