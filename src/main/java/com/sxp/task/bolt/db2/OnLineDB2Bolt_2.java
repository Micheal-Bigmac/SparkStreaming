package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.hbase.util.DateUtil;
import com.hsae.rdbms.db2.DB2ExcutorQuenue;
import com.sxp.task.protobuf.generated.OnLineInfo.OnLine;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.tuple.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OnLineDB2Bolt_2 extends BaseRichBolt {
	private static final Logger LOG = LoggerFactory.getLogger(OnLineDB2Bolt_2.class);
	private Map<Long, Long> cache = null;
	private boolean  flag=false;
	private static Connection connection = null;
	
	public void execute(Tuple input) {
		@SuppressWarnings("unchecked")
		List<byte[]> dataList = (List<byte[]>) input.getValue(0);
		List<String> sqls = null;
//		_collector.ack(input);
		long start = 0;
		if (dataList.size() == 0)
			return;
		try {
			sqls = buildPreparedSqlAndValuesList(dataList);
			start = System.currentTimeMillis();
			if (sqls == null || sqls.size() == 0) {
				LOG.warn("no preparedSqlAndValuesList can be built from " + dataList.size() + " dataList.");
			} else {
				DB2ExcutorQuenue.excuteBatch(sqls,connection);
			}
		} catch (InvalidProtocolBufferException e) {
			LOG.error(this.getClass().getName() + ".execute error!");
		} catch (IllegalArgumentException e) {
//			_collector.fail(input);
			LOG.error(this.getClass().getName() + ".execute error!");
		} catch (SQLException e) {
			if (sqls != null && sqls.size() > 0) {
				for(String tmp : sqls){
					LOG.error(tmp);
				}
//				_collector.emit("404", new Values(sqls));
			}
		} finally {
			LOG.warn("execute sql for " + dataList.size() + " records in " + ((System.currentTimeMillis() - start))
					+ "ms");
			LOG.warn("cache size " + cache.size() + "上下线信息");
		}

	}

	public void prepare(Map stormConf) {
		cache = new HashMap<Long, Long>();
		connection = DB2ExcutorQuenue.isExstConnection(connection);
	}


	protected List<String> buildPreparedSqlAndValuesList(List<byte[]> onlineList)
			throws InvalidProtocolBufferException {
		if (onlineList.size() == 0)
			return null;
		byte[] dataContent = null;
		byte[] bs = null;
		byte[] vehicle = null;
		List<String> sqls = new ArrayList<String>();
		StringBuffer Condition1_Sql1 = null;
		StringBuffer Condition1_Sql2 = null;
		StringBuffer Condition1_Sql3 = null;

		StringBuffer Condition2_Sql1 = null;
		StringBuffer Condition2_Sql2 = null;
		String date = null;

		String F_ENTERPRISE_CODE = null;
		long deviceRegID;
		long terminalid;
		OnLine online = null;
		for (int i = 0, size = onlineList.size(); i < size; i++) {
			vehicle = new byte[8];
			bs = onlineList.get(i);
			System.arraycopy(bs, 4, vehicle, 0, 8);
			long vehicleId = Bytes.toLong(vehicle);
			dataContent = new byte[bs.length - 12];
			// 数组之间的复制
			System.arraycopy(bs, 12, dataContent, 0, bs.length - 12);
			try {
				online = OnLine.parseFrom(dataContent);
				deviceRegID = online.getDeviceRegID();
				terminalid = online.getTERMINALID();
				F_ENTERPRISE_CODE = online.getEnterpriseCode();
				long changeTime = online.getChangeTime();
				date = DateUtil.getStrTime(changeTime, "yyyy-MM-dd HH:mm:ss");
				if (online.getStatus()) {
					if (cache.containsKey(vehicle)) {
						long date2 = cache.get(vehicle);
						if (changeTime >= date2) {
							Condition1_Sql1 = new StringBuffer();
							Condition1_Sql1.append("UPDATE T_VEHICLE_ONLINE_LOG SET F_OFF_TIME = '");

							Condition1_Sql1.append(date).append("',F_REASON=0  WHERE F_VEHICLE_ID = ").append(vehicleId)
									.append(" and F_ONLINE_TIME< '").append(date).append("' and F_OFF_TIME is null");
							sqls.add(Condition1_Sql1.toString());

							Condition1_Sql2 = new StringBuffer();
							Condition1_Sql2.append(
									"INSERT INTO T_VEHICLE_ONLINE_LOG (F_VEHICLE_ID,F_TERMINAL_ID,F_ONLINE_TIME,F_ENTERPRISE_CODE) VALUES ");
							Condition1_Sql2.append("(").append(vehicleId).append(",").append(terminalid).append(",'")
									.append(date).append("','").append(F_ENTERPRISE_CODE).append("'");
							Condition1_Sql2.append(")");
							sqls.add(Condition1_Sql2.toString());

							Condition1_Sql3 = new StringBuffer();
							Condition1_Sql3.append("UPDATE T_TERMINAL_REGISTER ").append("SET F_ONLINE = '")
									.append(online.getStatus() ? 1 : 0).append("'").append(",F_AUTH_DATE ='")
									.append(date).append("'").append(" where F_ID = ").append(deviceRegID);
							sqls.add(Condition1_Sql3.toString());
							cache.put(vehicleId, changeTime);
						}
					} else {
						Condition1_Sql1 = new StringBuffer();
						Condition1_Sql1.append("UPDATE T_VEHICLE_ONLINE_LOG SET F_OFF_TIME = '");

						Condition1_Sql1.append(date).append("',F_REASON=0  WHERE F_VEHICLE_ID = ").append(vehicleId)
								.append(" and F_ONLINE_TIME< '").append(date).append("' and F_OFF_TIME is null");
						sqls.add(Condition1_Sql1.toString());

						Condition1_Sql2 = new StringBuffer();
						Condition1_Sql2.append(
								"INSERT INTO T_VEHICLE_ONLINE_LOG (F_VEHICLE_ID,F_TERMINAL_ID,F_ONLINE_TIME,F_ENTERPRISE_CODE) VALUES ");
						Condition1_Sql2.append("(").append(vehicleId).append(",").append(terminalid).append(",'")
								.append(date).append("','").append(F_ENTERPRISE_CODE).append("'");
						Condition1_Sql2.append(")");
						sqls.add(Condition1_Sql2.toString());

						Condition1_Sql3 = new StringBuffer();
						Condition1_Sql3.append("UPDATE T_TERMINAL_REGISTER ").append("SET F_ONLINE = '")
								.append(online.getStatus() ? 1 : 0).append("'").append(",F_AUTH_DATE ='").append(date)
								.append("'").append(" where F_ID = ").append(deviceRegID);
						sqls.add(Condition1_Sql3.toString());

						cache.put(vehicleId, changeTime);
					}
				} else {
					Condition2_Sql1 = new StringBuffer();
					Condition2_Sql1.append("UPDATE T_VEHICLE_ONLINE_LOG SET F_OFF_TIME ='").append(date)
							.append("',F_REASON = '").append(online.getReason()).append("' WHERE F_VEHICLE_ID =")
							.append(vehicleId).append(" and F_ONLINE_TIME< '").append(date)
							.append("' and F_OFF_TIME is null");
					sqls.add(Condition2_Sql1.toString());
					Condition2_Sql2 = new StringBuffer();
					Condition2_Sql2.append("UPDATE T_TERMINAL_REGISTER SET F_ONLINE = '")
							.append(online.getStatus() ? 1 : 0).append("',F_UPDATE_DATE ='").append(date)
							.append("',F_LAST_OFFTIME='").append(date).append("' where F_ID =").append(deviceRegID);
					sqls.add(Condition2_Sql2.toString());
				}
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse OnLine error", e);
			}
		}
		return sqls;
	}
}