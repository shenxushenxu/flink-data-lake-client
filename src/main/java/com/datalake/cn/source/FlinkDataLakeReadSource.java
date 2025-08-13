package com.datalake.cn.source;

import com.datalake.cn.client.DataLakeClient;
import com.datalake.cn.entity.DataLakeStreamData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;

public class FlinkDataLakeReadSource implements Serializable {


    public static DataStream<DataLakeStreamData> source(
            StreamExecutionEnvironment env,
            String dataLakeIp,
            int dataLakePort,
            String tableName,
            int readCount) throws Exception {


        int filnkParallelism = env.getParallelism();

        DataLakeClient dataLakeClient = new DataLakeClient(dataLakeIp, dataLakePort, tableName);
        int dataLakePartitionNum = dataLakeClient.getTableStructure().getPartitionNumber();
        dataLakeClient.close();


        Map<Integer, List<PartitionOffset>> map = new HashMap();

        for (int i = 0; i < dataLakePartitionNum; i++) {
            int index = 0;
            if (i < filnkParallelism) {
                index = i;
            } else {
                index = i - filnkParallelism;
            }

            List<PartitionOffset> list = null;
            if (map.containsKey(index)) {
                list = map.get(index);
                list.add(new PartitionOffset(i, -1));
            } else {
                list = new ArrayList<>();
                list.add(new PartitionOffset(i, -1));
            }
            map.put(index, list);
        }



        FlinkStreamRead flinkStreamRead = new FlinkStreamRead(dataLakeIp, dataLakePort, tableName, readCount, map);

        SingleOutputStreamOperator<DataLakeStreamData> dataLakeStreamDataDataStreamSource = env.addSource(flinkStreamRead).name(tableName).uid(tableName)
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

    public static DataStream<DataLakeStreamData> source(
            StreamExecutionEnvironment env,
            String dataLakeIp,
            int dataLakePort,
            String tableName,
            int readCount,
            Map<Integer, Long> offsetSave) throws Exception {


        int filnkParallelism = env.getParallelism();

        DataLakeClient dataLakeClient = new DataLakeClient(dataLakeIp, dataLakePort, tableName);
        int dataLakePartitionNum = dataLakeClient.getTableStructure().getPartitionNumber();
        dataLakeClient.close();


        Map<Integer, List<PartitionOffset>> map = new HashMap();

        for (int i = 0; i < dataLakePartitionNum; i++) {
            int index = 0;
            if (i < filnkParallelism) {
                index = i;
            } else {
                index = i - filnkParallelism;
            }

            Long offset = offsetSave.get(i);
            if (offset == null) {
                offset = -1L;
            }

            List<PartitionOffset> list = null;
            if (map.containsKey(index)) {
                list = map.get(index);
                list.add(new PartitionOffset(i, offset));
            } else {
                list = new ArrayList<>();
                list.add(new PartitionOffset(i, offset));
            }
            map.put(index, list);
        }


        FlinkStreamRead flinkStreamRead = new FlinkStreamRead(dataLakeIp, dataLakePort, tableName, readCount, map);

        SingleOutputStreamOperator<DataLakeStreamData> dataLakeStreamDataDataStreamSource = env.addSource(flinkStreamRead).name(tableName).uid(tableName)
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


class PartitionOffset implements Serializable {
    private final int partitionCode;
    private final long offset;

    public PartitionOffset(int partitionCode, long offset) {
        this.partitionCode = partitionCode;
        this.offset = offset;
    }

    public int getPartitionCode() {
        return this.partitionCode;
    }

    public long getOffset() {
        return this.offset;
    }


}
