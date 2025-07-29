package com.shenxu.cn.sink;

import com.shenxu.cn.client.DataLakeClient;
import com.shenxu.cn.entity.LineData;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DataLakeSave extends KeyedProcessFunction<Integer, LineData, String> {

    private DataLakeClient dataLakeClient = null;
    private String dataLakeIp;
    private int dataLakePort;
    private String tableName;
    private int partitionNum;
    private int count;
    private long time;

    int dataCount = 0;
    long timeing = 0;

    List<LineData> dataList = null;
//    Integer key = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        dataLakeClient = new DataLakeClient(dataLakeIp, dataLakePort);
//        key = getRuntimeContext().getIndexOfThisSubtask();
        dataList = new ArrayList<>();
    }

    @Override
    public void processElement(LineData value, KeyedProcessFunction<Integer, LineData, String>.Context ctx, Collector<String> out) throws Exception {


        dataList.add(value);

        if (dataCount == 0) {
            dataCount = 1;
            TimerService timerService = ctx.timerService();
            long thisTime = timerService.currentProcessingTime();
            long processingTimeTimer = thisTime + time;
            timerService.registerProcessingTimeTimer(processingTimeTimer);
            timeing = processingTimeTimer;
        } else {
            dataCount = dataCount + 1;
        }



        if (dataCount >= this.count) {

            Integer key = ctx.getCurrentKey();
            this.dataLakeSaveFunction(key);


            if (timeing != 0) {
                ctx.timerService().deleteProcessingTimeTimer(timeing);
            }

            out.collect("数量触发的：    key: " + key + "  , 发送了:  " + dataCount);

            dataCount = 0;
            timeing =0L;

        }


    }


    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Integer, LineData, String>.OnTimerContext ctx, Collector<String> out) throws Exception {

        Integer key = ctx.getCurrentKey();
        this.dataLakeSaveFunction(key);

        TimerService timerService = ctx.timerService();
        long thisTime = timerService.currentProcessingTime();
        long processingTimeTimer = thisTime + time;
        timerService.registerProcessingTimeTimer(processingTimeTimer);
        timeing = processingTimeTimer;

        out.collect("定时器触发的：   key: " + key + "  , 发送了:  " + dataCount);

        dataCount = 0;

    }

    @Override
    public void close() throws Exception {
        dataLakeClient.close();
    }

    public void dataLakeSaveFunction(int key) throws Exception {
        Iterator<LineData> iterator = dataList.iterator();
        while (iterator.hasNext()) {
            LineData lineData = iterator.next();
            dataLakeClient.putLineData(lineData);
        }

        dataLakeClient.execute(tableName, key);
        dataList.clear();
    }


    public DataLakeSave(String tableName, String dataLakeIp, int dataLakePort, int partitionNum, int count, long time) throws IOException {
        this.tableName = tableName;
        this.dataLakeIp = dataLakeIp;
        this.dataLakePort = dataLakePort;
        this.count = count;
        this.time = time;
        this.partitionNum = partitionNum;
    }


}
