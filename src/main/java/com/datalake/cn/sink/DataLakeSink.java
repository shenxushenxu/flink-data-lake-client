package com.datalake.cn.sink;


import com.datalake.cn.client.DataLakeClient;
import com.datalake.cn.entity.LineData;
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
implements OneInputStreamOperator<LineData, Void>, ProcessingTimeService.ProcessingTimeCallback{

    private DataLakeClient dataLakeClient = null;
    private String tableName;
    private String dataLakeIp;
    private int dataLakePort;
    private int countCondition;
    private long timeCondition;
    private List<String> insertColumnName;
    private ListState<LineData> listState;

    private List<LineData> buffer;

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
        buffer = new ArrayList();


        long currentTime = timerService.getCurrentProcessingTime();
        long sinkTime = currentTime + timeCondition;
        timerService.registerTimer(sinkTime, this);


    }

    @Override
    public void processElement(StreamRecord<LineData> element) throws Exception {

        LineData lineData = element.getValue();
        buffer.add(lineData);

        if (buffer.size() >= countCondition){
            this.saveData();
        }


    }


    @Override
    public void onProcessingTime(long time) throws IOException, InterruptedException, Exception {

        LocalDateTime currentDateTime = LocalDateTime.now();
        System.out.println("定时器执行:  当前时间是：" + currentDateTime);


        this.saveData();

        long currentTime = timerService.getCurrentProcessingTime();
        long sinkTime = currentTime + timeCondition;
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
        String uuid = UUID.randomUUID().toString();

        LOG.info(uuid+"插入了："+ buffer.size());
        for (LineData lineData : buffer){
            dataLakeClient.putLineData(lineData);
        }

        dataLakeClient.execute();
        buffer.clear();
        LOG.info(uuid+"完成插入");


    }


    @Override
    public void close() throws Exception {
        dataLakeClient.close();
    }
}
