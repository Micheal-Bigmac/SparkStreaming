package com.sxp.task.bolt;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.tuple.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @ClassName: IntervalMessageBolt
 * @Description: 从kafkaspout中读取数据并发送至后面的bolt，对数据进行过度
 * @author: 熊尧
 * @company: 上海势航网络科技有限公司
 * @date 2016年5月20日 下午3:52:01
 */
public class IntervalMessageBolt extends BaseRichBolt {

    private static final long serialVersionUID = 5822895113197753432L;

    private static final Logger LOG = LoggerFactory.getLogger(IntervalMessageBolt.class);
    public static final String OUTPUT_FIELD_NAME = "source";
    private Map<Integer, List<byte[]>> contentList = null;
    private long beforeTime = 0l;
    private static final int MaxIde = 10000;
    private boolean flag = false;

//	@Override
//	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
//		this.collector = collector;
//		contentList = new HashMap<Integer, List<byte[]>>();
//	}

    public void execute(Tuple input) {
        if (beforeTime == 0l) {
            beforeTime = System.currentTimeMillis();
        }
        if (flag) {
            contentList.clear();
            flag = false;
        }
        byte[] receivedData = (byte[]) input.getValueByField("msg");// msg为自定义消息Field

        // 消息类型id
        byte[] dataType = new byte[4];
        System.arraycopy(receivedData, 0, dataType, 0, 4);
        int dataTypeId = Bytes.toInt(dataType);
        // 消息内容
        byte[] dataContent = new byte[receivedData.length - 12];
        // 数组之间的复制
        System.arraycopy(receivedData, 12, dataContent, 0, receivedData.length - 12);

        // 添加到相应队列

        long currentTime = System.currentTimeMillis();
        List<byte[]> list = contentList.get(dataTypeId);
        if (list == null) {
            list = new ArrayList<byte[]>();
            if (dataTypeId == 100036 || dataTypeId == 100034)
                list.add(receivedData);
            else {
                list.add(dataContent);
            }

            contentList.put(dataTypeId, list);
        } else {
            if (dataTypeId == 100036 || dataTypeId == 100034)
                list.add(receivedData);
            else {
                list.add(dataContent);
            }
            contentList.put(dataTypeId, list);
        }
        if (contentList.size() > 20 || (currentTime - beforeTime) > MaxIde) {
            for (Integer integer : contentList.keySet()) {
//				collector.emit(integer.toString(), new Values(contentList.get(integer)));
                if (integer == 100003 || integer == 100001 || integer == 100005 || integer == 100034) {
//                    collector.emit(integer.toString(), new Values(contentList.get(integer)));
                } else {
                    List<byte[]> list2 = contentList.get(integer);
                    for (byte[] tmp : list2) {
                        List<byte[]> list3 = new ArrayList<>();
                        list3.add(tmp);
//                        collector.emit(integer.toString(), new Values(list3));
                    }
                }
            }
            beforeTime = currentTime;
            flag = true;
        }
//        this.collector.ack(input);
    }

//	@Override
//	public void declareOutputFields(OutputFieldsDeclarer declarer) {
//		SpoutStreamTypeId[] values = SpoutStreamTypeId.values();
//		for (int i = 0; i < values.length; i++) {
//			declarer.declareStream(values[i].getValue(), new Fields(OUTPUT_FIELD_NAME));
//		}
//	}
}
