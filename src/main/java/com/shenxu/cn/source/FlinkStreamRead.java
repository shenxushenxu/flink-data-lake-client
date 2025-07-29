package com.shenxu.cn.source;

import com.shenxu.cn.client.DataLakeStreamClient;
import com.shenxu.cn.entity.DataLakeStreamData;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class FlinkStreamRead extends RichSourceFunction<DataLakeStreamData> {


    private DataLakeStreamClient dataLakeStreamClient;
    private String dataLakeIp;
    private int dataLakePort;
    private String tableName;
    private int readCount;
    private MapState<Integer, Long> offsetSave = null;
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


        MapStateDescriptor<Integer, Long> offsetSaveState =
                new MapStateDescriptor<Integer, Long>("offsetSave", Integer.class, Long.class);
        offsetSave = getRuntimeContext().getMapState(offsetSaveState);

    }

    public void setPartitionCodeAndOffSet(int partitionCode, long offset){
        dataLakeStreamClient.setPartitionCodeAndOffSet(partitionCode, offset);
    }


    @Override
    public void run(SourceContext<DataLakeStreamData> ctx) throws Exception {

        while (true){
            Iterator<Map.Entry<Integer, Long>> iterator = offsetSave.iterator();

            while (iterator.hasNext()){
                int partitionCode = iterator.next().getKey();
                long offset = iterator.next().getValue();
                dataLakeStreamClient.setPartitionCodeAndOffSet(partitionCode, offset);
            }


            List<DataLakeStreamData> list = dataLakeStreamClient.load();

            for (DataLakeStreamData dataLakeStreamData : list){

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
}
