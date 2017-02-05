package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.hbase.util.DateUtil;
import com.hsae.rdbms.db2.DB2ExcutorQuenue;
import com.sxp.task.protobuf.generated.GpsInfo.AlarmItem;
import com.sxp.task.protobuf.generated.GpsInfo.Gps;
import org.apache.storm.tuple.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AlarmDB2Bolt_2 extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(AlarmDB2Bolt.class);
	static String TABLE_NAME = "T_ALARM_";
	static String TABLE_ERROR_NAME = "T_ALARM_ERR";
	private static Connection connection = null;

	public void execute(Tuple input) {
		@SuppressWarnings("unchecked")
		List<byte[]> dataList = (List<byte[]>) input.getValue(0);
		long start = 0;
//		_collector.ack(input);
		List<String> sql = null;
		if (dataList.size() == 0)
			return;
		try {
			sql = buildPreparedSqlAndValuesList(dataList);
			start = System.currentTimeMillis();
			if (sql == null || sql.size() == 0) {
				LOG.warn("no preparedSqlAndValuesList can be built from " + dataList.size() + " dataList.");
			} else {
				DB2ExcutorQuenue.excuteBatch(sql,connection);
			}
		} catch (InvalidProtocolBufferException e) {
			LOG.error(this.getClass().getName() + ".execute error!");
		} catch (IllegalArgumentException e) {
			LOG.error(this.getClass().getName() + ".execute error!");
		} catch (SQLException e) {
//			if ( sql != null && sql.size() > 0)
		} finally {
			LOG.warn("execute sql for " + dataList.size() + " records in " + ((System.currentTimeMillis() - start)) + "ms");
		}
	}

	protected List<String> buildPreparedSqlAndValuesList(List<byte[]> gpsList) throws InvalidProtocolBufferException {
		List<String> Sqls = new ArrayList<String>();
		StringBuffer GpsSql = null;
		Gps gps = null;
		long gpsDate;
		String gpsTime = null;
		String TABLE_NAME_DATE = null;
		for (int i = 0, size = gpsList.size(); i < size; i++) {
			try {
				gps = Gps.parseFrom(gpsList.get(i));

				List<AlarmItem> items = gps.getAlarmItemsList();
				// 对接收的数据进行预处理，GPS时间不是两天之内的插入对应的ERR表，否则插入对应的天维度表
				gpsDate = gps.getGpsDate();
				if (DateUtil.subtractOneDay() <= gpsDate && DateUtil.addOneDay() >= gpsDate) {
					gpsTime = DateUtil.getStrTime(gpsDate, "yyyyMMdd");
					TABLE_NAME_DATE = TABLE_NAME + gpsTime;
					for (AlarmItem alarmItem : items) {
						GpsSql = new StringBuffer();

						GpsSql.append("INSERT INTO ").append(TABLE_NAME_DATE).append(
								"(F_ID,F_LOCATION_ID,F_ALARM_ID,F_VEHICLE_ID,F_TERMINAL_ID,F_TIME,F_PLATE_CODE,F_ENTERPRISE_CODE) VALUES (");
						SqlPackage(GpsSql, alarmItem, gps, Sqls);
					}
				} else {
					for (AlarmItem alarmItem : items) {
						GpsSql = new StringBuffer();
						GpsSql.append(
								"INSERT INTO T_ALARM_ERR(F_ID,F_LOCATION_ID,F_ALARM_ID,F_VEHICLE_ID,F_TERMINAL_ID,F_TIME,F_PLATE_CODE,F_ENTERPRISE_CODE) VALUES (");
						SqlPackage(GpsSql, alarmItem, gps, Sqls);
					}
				}

			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse Gps error", e);
			}
		}
		return Sqls;
	}

	private void SqlPackage(StringBuffer alarmSql, AlarmItem alarmItem, Gps gps, List<String> Sqls) {
		String date = DateUtil.getStrTime(gps.getGpsDate(), "yyyy-MM-dd HH:mm:ss");
		alarmSql.append(alarmItem.getAlarmID()).append(",").append(gps.getLocationID()).append(",")
				.append(alarmItem.getAlarmType()).append(",").append(gps.getVehicleID()).append(",")
				.append(gps.getTerminalID()).append(",'").append(date).append("','").append(gps.getVehicleName())
				.append("','").append(gps.getEnterpriseCode()).append("')");
		Sqls.add(alarmSql.toString());
	}

	public void prepare(Map stormConf) {
		connection = DB2ExcutorQuenue.isExstConnection(connection);
	}


}
