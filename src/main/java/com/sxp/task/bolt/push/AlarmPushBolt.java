package com.sxp.task.bolt.push;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.tuple.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.hbase.util.DateUtil;
import com.sxp.task.protobuf.generated.GpsInfo.AlarmItem;
import com.sxp.task.protobuf.generated.GpsInfo.Gps;
import cn.jpush.api.common.resp.APIConnectionException;
import cn.jpush.api.common.resp.APIRequestException;

public class AlarmPushBolt extends BaseRichBolt {

	private static final Logger LOG = LoggerFactory.getLogger(AlarmPushBolt.class);
	private final String ALARMTYPE_CONF = "alarmType.conf";
	private Map<String, Object> alarmMap = null;
	Map<String, String> content = new HashMap<String, String>();
	private RedisUtil redisUtil = new RedisUtil();
	@SuppressWarnings("unchecked")
	public void prepare(Map stormConf) {
		this.alarmMap = (Map<String, Object>) stormConf.get(ALARMTYPE_CONF);
	}

	public void execute(Tuple input) {
		@SuppressWarnings("unchecked")
		List<byte[]> dataList = (List<byte[]>) input.getValue(0);
		Map<String, String> content = getContent(dataList);
		JPushManager jPushManager = JPushManager.getJPushManagerInstance();
		if (content != null && content.size() != 0) {
			for (String string : content.keySet()) {
				String[] str = string.split(",");
				String pushId = str[0];
				String vehicleId = str[1];
				String plateCode = str[2];
				try {
					jPushManager.Push(pushId, content.get(string), vehicleId, plateCode);
				} catch (APIConnectionException e) {
					LOG.error("Connection error, should retry later");
				} catch (APIRequestException e) {
					LOG.error("Should review the error, and fix the request");
				}
			}
		}
		
		content.clear();
	}

	public Map<String, String> getContent(List<byte[]> messages) {
		Gps gps = null;
		try {
			for (byte[] message : messages) {
				if (message != null) {
					// 对gps的每一条数据进行解析
					gps = Gps.parseFrom(message);
					long vehicleId = gps.getVehicleID();
					long dateTime = gps.getGpsDate();
					String date = DateUtil.toString(new Date(dateTime), DateUtil.DEFAULT_DATETIME_FORMAT_SEC);
					// 解析redis里相应的json
					Vehicle vehicle = redisUtil.getVehicle("VEHI:" + String.valueOf(vehicleId));
					List<AlarmItem> alarmItems = gps.getAlarmItemsList();

					// 以下为测试代码
//					List<AlarmItem> alarmItems = new ArrayList<>();
//					AlarmItem.Builder builder = AlarmItem.newBuilder();
//					builder.setAlarmID(33l);
//					builder.setAlarmType(33l);
//					AlarmItem.Builder builder1 = AlarmItem.newBuilder();
//					builder1.setAlarmID(1l);
//					builder1.setAlarmType(1l);
//					alarmItems.add(builder.build());
//					alarmItems.add(builder1.build());

					// gps中有报警并且redis存在相关的用户信息，才进行推送报警信息
					if (alarmItems != null && alarmItems.size() != 0) {
						if (vehicle != null) {
							String plageCode = vehicle.getPlateCode();
							Map<String, User> users = vehicle.getUserMap();
							if (null != users && users.size() != 0) {
								for (String userStr : users.keySet()) {
									String user = users.get(userStr).getFullName();
									String redisAlarmType = users.get(userStr).getAlarmList();
									String alarm = "";
									// 根据redis中订阅的报警信息拼接报警信息
									if(redisAlarmType != "" && redisAlarmType != null){
										for (AlarmItem alarmItem : alarmItems) {
											for(String string : redisAlarmType.split(","))
											if ((alarmItem.getAlarmType() + "").equals(string)) {
												alarm = alarm + alarmMap.get(("A" + alarmItem.getAlarmType())) + ",";
											}
										}
									}
									
									if (alarm != null && alarm != "") {
										alarm = alarm.substring(0, alarm.length() - 1);
										String alertMessage = "亲爱的用户：" + user + "您好，车牌号为" + plageCode + "于" + date
												+ "产生了报警，报警类型为:" + alarm;
										Map<String, String> pushIds = users.get(userStr).getDeviceMap();
										if (pushIds != null && pushIds.size() != 0) {
											for (String pushId : pushIds.keySet())
												content.put((pushIds.get(pushId) + "," + vehicleId + "," + plageCode), alertMessage);
										}
									}
									
								}
							}
						}
					}
				}
			}
		} catch (InvalidProtocolBufferException e) {
			LOG.error("Protobuf parse Alarm error", e);
		}
		return content;
	}

}
