package com.datalake.cn.source;


import com.datalake.cn.client.DataLakeStreamClient;
import com.datalake.cn.entity.DataLakeStreamData;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.IOException;
import java.util.*;

public class FlinkStreamRead extends RichSourceFunction<DataLakeStreamData> implements CheckpointedFunction {


    private DataLakeStreamClient dataLakeStreamClient;
    private String dataLakeIp;
    private int dataLakePort;
    private String tableName;
    private int readCount;
    private ListState<String> listState;
    private Map<Integer, Long> offsetSave = null;


    //    private MapState<Integer, Long> offsetSave = null;
    public FlinkStreamRead(String dataLakeIp, int dataLakePort, String tableName, int readCount) throws IOException {
        this.dataLakeIp = dataLakeIp;
        this.dataLakePort = dataLakePort;
        this.tableName = tableName;
        this.readCount = readCount;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        dataLakeStreamClient = new DataLakeStreamClient(dataLakeIp, dataLakePort);
        dataLakeStreamClient.setReadCount(readCount);
        dataLakeStreamClient.setTableName(tableName);

        if (offsetSave != null) {
            Set<Integer> keys = offsetSave.keySet();
            for (Integer partitionCode : keys) {
                Long offset = offsetSave.get(partitionCode);
                dataLakeStreamClient.setPartitionCodeAndOffSet(partitionCode, offset);
            }
        }
    }

    public void setPartitionCodeAndOffSet(int partitionCode, long offset) {
        dataLakeStreamClient.setPartitionCodeAndOffSet(partitionCode, offset);
    }


    @Override
    public void run(SourceContext<DataLakeStreamData> ctx) throws Exception {

        while (true) {

            List<DataLakeStreamData> list = dataLakeStreamClient.load();
            offsetSave = dataLakeStreamClient.getOffsetSave();
            for (DataLakeStreamData dataLakeStreamData : list) {

                int partitionCode = dataLakeStreamData.getPartitionCode();
                long offset = dataLakeStreamData.getOffset();

                offsetSave.put(partitionCode, offset);

                ctx.collect(dataLakeStreamData);
            }
        }
    }

    @Override
    public void cancel() {

        try {
            dataLakeStreamClient.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (!offsetSave.isEmpty()) {

            Set<Integer> keys = offsetSave.keySet();
            for (Integer partitionCode : keys) {
                Long offset = offsetSave.get(partitionCode);
                String value = partitionCode + "_" + offset;
                listState.add(value);
            }
        }

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<String> descriptor = new ListStateDescriptor<>("state", String.class);
        listState = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            offsetSave = new HashMap<>();
            for (String element : listState.get()) {
                String[] parOffset = element.split("_");

                offsetSave.put(Integer.valueOf(parOffset[0]), Long.valueOf(parOffset[1]));
            }
        }
    }
}
