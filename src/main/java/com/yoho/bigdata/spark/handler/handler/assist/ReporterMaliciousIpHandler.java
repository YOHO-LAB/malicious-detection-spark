package com.yoho.bigdata.spark.handler.handler.assist;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yoho.bigdata.spark.config.PropertiesContext;
import com.yoho.bigdata.spark.model.EventMessage;
import com.yoho.bigdata.spark.model.ReportBo;
import com.yoho.bigdata.spark.model.UserIPDetailStat;
import com.yoho.bigdata.spark.utils.HBasePool;
import com.yoho.bigdata.spark.utils.HttpClientUtil;

import jersey.repackaged.com.google.common.collect.Lists;

/**
 * 描述:
 * Created by pangjie@yoho.cn on 2017/8/25.
 */
public class ReporterMaliciousIpHandler implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(ReporterMaliciousIpHandler.class);

    private PropertiesContext pc;

    public static final String ALARM_TYPE = "alarm";

    public static final String FORBID_TYPE = "forbid";

    public ReporterMaliciousIpHandler(PropertiesContext pc) {
        this.pc = pc;
    }

    private static ExecutorService pool= Executors.newFixedThreadPool(5);


    public void reportForbidIp(Iterator<ReportBo> reportBos) {

        try {
            if (reportBos == null) {
                return;
            }

            List<String> ipsList = new ArrayList<>();
            List<ReportBo> reportBoList = new ArrayList<>();
            while (reportBos.hasNext()) {
                ReportBo reportBo = reportBos.next();
                reportBoList.add(reportBo);
                ipsList.add(reportBo.getIp());
            }
            if (ipsList.size() == 0) {
                return;
            }
            
            boolean isForbid=null==pc.getForbid()?true:pc.getForbid().isFlag();
            //上报到Nginx
            if(isForbid){
                reportMaliciousIp2Nginx(ipsList);
            }

            //上报到运维平台
            reportMaliciousIp2Ops(reportBoList, FORBID_TYPE);

            //上报到UIC
            if(isForbid){
                reportMaliciousIp2Uic(ipsList);
            }

        } catch (Exception e) {
        }
    }


    //只上报运维平台
    public void reportAlarmIp(Iterator<ReportBo> reportBos) {

        try {
            if (reportBos == null) {
                return;
            }
            List<ReportBo> reportBoList = new ArrayList<>();
            while (reportBos.hasNext()) {
                ReportBo reportBo = reportBos.next();
                reportBoList.add(reportBo);
            }
            if (reportBoList.size() == 0) {
                return;
            }
            //上报到运维平台
            reportMaliciousIp2Ops(reportBoList, ALARM_TYPE);

        } catch (Exception e) {
        }
    }


    private void reportMaliciousIp2Nginx(List<String> ips) {
        if (ips == null || ips.size() == 0) {
            return;
        }
        try {
            List<String> nginxIps = getNginxIp();
            String ipsString = StringUtils.join(ips, ",");
            for (String nginx : nginxIps) {
                try {
                    //用 erp.yoho.yohoops.org 域名增加Host头
                    Header header = new BasicHeader("Host", "erp.yoho.yohoops.org");
                    HttpClientUtil.get("http://" + nginx + "/malIp?method=add&expire=3600&ips=" + ipsString, header);
                } catch (Exception e) {
                    System.out.println("reportMaliciousIp2Nginx exception is:"+e);
                    continue;
                }
            }
        } catch (Exception e) {

        }

    }
    //http://www.yohops.com/outer/getHostIpByTags?tags=upstream-switch
	private List<String> getNginxIp() {
		String ops = pc.getCallBackProperties().getOps();
		String s = HttpClientUtil.get("http://" + ops + "/outer/getHostIpByTags?tags=upstream-switch");
		if (StringUtils.isBlank(s)) {
			return pc.getCallBackProperties().getNginx();
		}
		List<String> nginxIps=Lists.newArrayList();
		try {
			nginxIps = JSON.parseArray(s, String.class);
		} catch (Exception e) {
			
		}
		if (CollectionUtils.isEmpty(nginxIps)) {
			return pc.getCallBackProperties().getNginx();
		}
		return nginxIps;
	}

    private void reportMaliciousIp2Ops(List<ReportBo> reportBoList, String type) {
        String opsIp = pc.getCallBackProperties().getOps();
        String maliciousIp=pc.getCallBackProperties().getMaliciousIp();

        if (opsIp == null || reportBoList == null || reportBoList.size() == 0) {
            return;
        }
        HTable resultTable =null;
        try {
            //上报ops
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("type", type);
            jsonObject.put("data", reportBoList);
			jsonObject.put("idc", null == pc.getIDC() ? "" : pc.getIDC().getFlag());
			
            String s = HttpClientUtil.postJson("http://" + maliciousIp + "/outer/saveMaliciousIp", jsonObject);
            //详细数据存入hbase，方便查看
            resultTable = (HTable) HBasePool.getConnection(pc).getTable(TableName.valueOf("malicious_detection"));
            List<Put> putList = new ArrayList<>();
            for (ReportBo reportBo : reportBoList) {
                String ip = reportBo.getIp();
                List<EventMessage> messageList = reportBo.getMessages();
                if (messageList == null)
                    continue;
                //hbase存储按照字典顺序，所有序号前要补0
                int length = String.valueOf(messageList.size()).length();
                String format = "%0" + length + "d";
                for (int i = 0; i < messageList.size(); i++) {
                    Put put = new Put(Bytes.toBytes(ip + ":" + reportBo.getTimestamp() + ":" + String.format(format, i)));
                    put.addColumn(Bytes.toBytes("data"), Bytes.toBytes(type), Bytes.toBytes(messageList.get(i).getOriginalMessage()));
                    putList.add(put);
                }
            }
            resultTable.put(putList);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }finally {
            HBasePool.closeTable(resultTable);
        }
    }


    public void insertHbaseNoReport(Iterator<UserIPDetailStat> userIPDetailStatIterator) {
        if (userIPDetailStatIterator == null || !pc.getHbase().isWriteHbase()) {
            return;
        }
        List<UserIPDetailStat> userIPDetailStatList=Lists.newArrayList();
        
        while (userIPDetailStatIterator.hasNext()) {
            UserIPDetailStat userIPDetailStat = userIPDetailStatIterator.next();
            userIPDetailStatList.add(userIPDetailStat);
        }
        pool.execute(new Runnable() {
            @Override
            public void run() {
                HTable resultTable = null;
                try {
                    resultTable = (HTable) HBasePool.getConnection(pc).getTable(TableName.valueOf("all_detection"));
                    List<Put> putList = new ArrayList<>();
                    for (UserIPDetailStat userIPDetailStat:userIPDetailStatList){
                        String ip = userIPDetailStat.getIp();
                        List<EventMessage> messageList = userIPDetailStat.getMessages();
                        if (messageList == null)
                            continue;
                        //hbase存储按照字典顺序，所有序号前要补0
                        int length = String.valueOf(messageList.size()).length();
                        String format = "%0" + length + "d";

                        for (int i = 0; i < messageList.size(); i++) {
                            Put put = new Put(Bytes.toBytes(ip + ":" + userIPDetailStat.getTimestamp() + ":" + String.format(format, i)));
                            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("message"), Bytes.toBytes(messageList.get(i).getOriginalMessage()));
                            putList.add(put);
                        }
                    }
                    resultTable.put(putList);
                    logger.info("finish to do insertHbaseNoReport, size:{}", putList.size());
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                } finally {
                    HBasePool.closeTable(resultTable);
                }
            }
        });
    }

    private void reportMaliciousIp2Uic(List<String> ips) {
        List<String> uicIps = pc.getCallBackProperties().getUic();
        if (CollectionUtils.isEmpty(uicIps)) {
            return;
        }
        try {
            for (String uic : uicIps) {
                for (String ip : ips) {
                    try {
                        String result=HttpClientUtil.get("http://" + uic + "/addMaliciousIp?ip=" + ip + "&expiretime=86400");
                    } catch (Exception e) {
                    		System.out.println("reportMaliciousIp2Uic exception is:"+e);
                    }
                }
            }
        } catch (Exception e) {
        }
    }
}
