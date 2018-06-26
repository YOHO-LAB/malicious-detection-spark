package com.yoho.bigdata.spark.listener;

import com.yoho.bigdata.spark.utils.HttpClientUtil;
import org.apache.spark.streaming.scheduler.*;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 任务执行延时监控
 */
public class MaliciousDetectStreamingListener implements StreamingListener {

    private  int duration;
    private  int threshold;

    private static AtomicInteger totalAlarm=new AtomicInteger(0);

    public MaliciousDetectStreamingListener(int duration){
        this.duration=duration;
        //延迟10个批次就需要告警了
        this.threshold=duration*10*1000;
    }
    @Override
    public void onBatchCompleted(StreamingListenerBatchCompleted batchCompleted) {
        if(null!=batchCompleted.batchInfo()){
            Long totalDelay=Long.valueOf(batchCompleted.batchInfo().totalDelay().get().toString());
            Long processingDelay=Long.valueOf(batchCompleted.batchInfo().processingDelay().get().toString());
            if(totalDelay>threshold){
                String info ="MaliciousDetect"+duration+",totalDelay:"+totalDelay+",processingDelay:" + processingDelay;
                String url="http://10.67.4.2:1979/bigDataAlarm?info="+info+"&mobiles=18651650543,18652925653,18021522255";
                int count=totalAlarm.get();
                //避免频繁告警
                if(count%500==0){
                    HttpClientUtil.get(url);
                }
                totalAlarm.incrementAndGet();
                //定时归0，避免这个太大溢出
                if(totalAlarm.get()>10000){
                    totalAlarm.set(0);
                }
            }
        }
    }

    public static void main(String[] args) {
        String info = "MaliciousDetect" + 1 + ",totalDelay:" + 2 + ",processingDelay:" + 3;
        String url = "http://10.67.4.2:1979/bigDataAlarm?info=" + info + "&mobiles=18651650543,18652925653,18021522255";
        HttpClientUtil.get(url);
    }

    @Override
    public void onOutputOperationStarted(StreamingListenerOutputOperationStarted outputOperationStarted) {
        //nothing to do
    }

    @Override
    public void onOutputOperationCompleted(StreamingListenerOutputOperationCompleted outputOperationCompleted) {
        //nothing to do
    }

    @Override
    public void onReceiverStarted(StreamingListenerReceiverStarted receiverStarted) {
        //nothing to do
    }

    @Override
    public void onReceiverError(StreamingListenerReceiverError receiverError) {
        //nothing to do
    }

    @Override
    public void onReceiverStopped(StreamingListenerReceiverStopped receiverStopped) {
        //nothing to do

    }

    @Override
    public void onBatchSubmitted(StreamingListenerBatchSubmitted batchSubmitted) {
        //nothing to do
    }

    @Override
    public void onBatchStarted(StreamingListenerBatchStarted batchStarted) {
        //nothing to do
    }
}
