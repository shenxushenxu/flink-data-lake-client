package com.datalake.cn.sink;


import com.datalake.cn.client.DataLakeClient;
import com.datalake.cn.entity.BatchData;
import com.datalake.cn.entity.DataLakeLinkData;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class DataLakeSink extends AbstractStreamOperator<Void>
implements OneInputStreamOperator<DataLakeLinkData, Void>, ProcessingTimeService.ProcessingTimeCallback{

    private DataLakeClient dataLakeClient = null;
    private String tableName;
    private String dataLakeIp;
    private int dataLakePort;
    private int countCondition;
    private long timeCondition;
    private List<String> insertColumnName;
    private ListState<BatchData> listState;

    private BatchData buffer;

    private ProcessingTimeService timerService;



    public DataLakeSink(String tableName, String dataLakeIp, int dataLakePort,List<String> insertColumnName, int count, long time) throws IOException {

        this.tableName = tableName;
        this.dataLakeIp = dataLakeIp;
        this.dataLakePort = dataLakePort;
        this.countCondition = count;
        this.timeCondition = time;
        this.insertColumnName = insertColumnName;
    }


    @Override
    public void open() throws Exception {

        dataLakeClient = new DataLakeClient(dataLakeIp, dataLakePort, tableName, insertColumnName);
        timerService = getProcessingTimeService();
        buffer = new BatchData(insertColumnName);


        long currentTime = timerService.getCurrentProcessingTime();
        long sinkTime = currentTime + timeCondition;
        timerService.registerTimer(sinkTime, this);


    }

    @Override
    public void processElement(StreamRecord<DataLakeLinkData> element) throws Exception {

        DataLakeLinkData lineData = element.getValue();
        buffer.putDataLakeLinkData(lineData);

        if (buffer.getSize() >= countCondition){
            this.saveData();
        }


    }


    @Override
    public void onProcessingTime(long time) throws IOException, InterruptedException, Exception {

        this.saveData();

        long currentTime = timerService.getCurrentProcessingTime();
        long sinkTime = currentTime + timeCondition;
        timerService.registerTimer(sinkTime, this);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        ListStateDescriptor<BatchData> listDescriptor = new ListStateDescriptor<>("buffer-state",BatchData.class);

        // 获取或创建状态
        listState = context.getOperatorStateStore().getListState(listDescriptor);
        // 从状态恢复数据（故障恢复）
        if (context.isRestored()) {
            for (BatchData element : listState.get()) {
                buffer = element;
            }
        }

    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        if (buffer.getSize() != 0){
            listState.add(buffer);
        }
    }

    private void saveData() throws Exception {

        dataLakeClient.putBatchData(buffer);
        dataLakeClient.execute();
    }


    @Override
    public void close() throws Exception {
        dataLakeClient.close();
    }
}
