package com.shenxu.cn.sink;

import com.shenxu.cn.client.DataLakeClient;
import com.shenxu.cn.entity.LineData;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DataLakeSink extends AbstractStreamOperator<Void>
implements OneInputStreamOperator<LineData, Void>, ProcessingTimeService.ProcessingTimeCallback{

    private DataLakeClient dataLakeClient = null;
    private String tableName;
    private String dataLakeIp;
    private int dataLakePort;
    private int countCondition;
    private long timeCondition;

    private ListState<LineData> listState;

    private List<LineData> buffer;

    private ProcessingTimeService timerService;

    private long sinkTime = 0;

    public DataLakeSink(String tableName, String dataLakeIp, int dataLakePort, int count, long time) throws IOException {

        this.tableName = tableName;
        this.dataLakeIp = dataLakeIp;
        this.dataLakePort = dataLakePort;
        this.countCondition = count;
        this.timeCondition = time;
    }


    @Override
    public void open() throws Exception {
        dataLakeClient = new DataLakeClient(dataLakeIp, dataLakePort);
        timerService = getProcessingTimeService();
        buffer = new ArrayList();
    }

    @Override
    public void processElement(StreamRecord<LineData> element) throws Exception {

        if (buffer.size() == 0 && sinkTime == 0){
            long currentTime = timerService.getCurrentProcessingTime();
            sinkTime = currentTime + timeCondition;

            timerService.registerTimer(sinkTime, this);
        }


        LineData lineData = element.getValue();
        buffer.add(lineData);


        if (buffer.size() >= countCondition){
            this.saveData();

        }


    }


    @Override
    public void onProcessingTime(long time) throws IOException, InterruptedException, Exception {
        this.saveData();

        long currentTime = timerService.getCurrentProcessingTime();
        sinkTime = currentTime + time;
        timerService.registerTimer(sinkTime, this);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        ListStateDescriptor<LineData> listDescriptor = new ListStateDescriptor<>("buffer-state",LineData.class);

        // 获取或创建状态
        listState = context.getOperatorStateStore().getListState(listDescriptor);
        // 从状态恢复数据（故障恢复）
        if (context.isRestored()) {
            buffer = new ArrayList<>();
            for (LineData element : listState.get()) {
                buffer.add(element);
            }
        }

    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        if (!buffer.isEmpty()){
            for (LineData lineData : buffer){
                listState.add(lineData);
            }
        }
    }

    private void saveData() throws Exception {
        LOG.info("插入了："+ buffer.size());
        for (LineData lineData : buffer){
            dataLakeClient.putLineData(lineData);
        }

        dataLakeClient.execute(tableName);
        buffer.clear();



    }


    @Override
    public void close() throws Exception {
        dataLakeClient.close();
    }
}
