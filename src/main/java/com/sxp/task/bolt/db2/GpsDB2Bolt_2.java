package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.hbase.util.DateUtil;
import com.hsae.rdbms.db2.DB2ExcutorQuenue;
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

public class GpsDB2Bolt_2 extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(GpsDB2Bolt_2.class);

	static String TABLE_NAME = "T_LOCATION_";
	static String TABLE_ERROR_NAME = "T_LOCATION_ERR";
	private static Connection connection = null;

	public void execute(Tuple input) {
		@SuppressWarnings("unchecked")
		List<byte[]> dataList = (List<byte[]>) input.getValue(0);
		long start = 0;
//		_collector.ack(input);
		if (dataList.size() == 0)
			return;
		List<String> sql = null;
		try {
			sql = buildPreparedSqlAndValuesList(dataList);
			start = System.currentTimeMillis();
			if (sql == null || sql.size() == 0) {
				LOG.warn("no preparedSqlAndValuesList can be built from " + dataList.size() + " dataList.");
			} else {
				DB2ExcutorQuenue.excuteBatch(sql,connection);
			}
		} catch (InvalidProtocolBufferException e) {
//			_collector.fail(input);
			LOG.error(this.getClass().getName() + ".execute error!");
		} catch (IllegalArgumentException e) {
//			_collector.fail(input);
			LOG.error(this.getClass().getName() + ".execute error!");
		} catch (SQLException e) {
			if (sql != null && sql.size() > 0) {
//				_collector.emit("404", new Values(sql));
			}
		} finally {
			LOG.warn("execute sql for " + dataList.size() + " records in " + ((System.currentTimeMillis() - start))
					+ "ms");
		}

	}

	public void prepare(Map stormConf) {
		connection = DB2ExcutorQuenue.isExstConnection(connection);
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

				// 对接收的数据进行预处理，GPS时间不是两天之内的插入对应的ERR表，否则插入对应的天维度表
				gpsDate = gps.getGpsDate();
				if (DateUtil.subtractOneDay() <= gpsDate && DateUtil.addOneDay() >= gpsDate) {
					GpsSql = new StringBuffer();
					gpsTime = DateUtil.getStrTime(gpsDate, "yyyyMMdd");
					TABLE_NAME_DATE = TABLE_NAME + gpsTime;
					GpsSql.append("INSERT INTO ").append(TABLE_NAME_DATE).append(
							"(F_ID,F_VEHICLE_ID,F_LATITUDE,F_LONGITUDE,F_HIGH,F_SPEED,F_DIRECTION,F_ALARM_COUNT,F_DSPEED,F_OIL_LEVEL,F_MILEAGE,F_ALARM_DATA,F_GPS_TIME,F_RECV_TIME,F_ACC_STATUS,F_IS_REAL_LOCATION,F_TERMINAL_ID,F_ENTERPRISE_CODE,F_GPSENCRYPT,F_GPSSTATE,F_ISPASSUP,F_ALARM_DATA1,F_AREASN) VALUES (");
					SqlPackage(GpsSql, gps,Sqls);
				} else {
					GpsSql = new StringBuffer();
					GpsSql.append(
							"INSERT INTO T_LOCATION_ERR(F_ID,F_VEHICLE_ID,F_LATITUDE,F_LONGITUDE,F_HIGH,F_SPEED,F_DIRECTION,F_ALARM_COUNT,F_DSPEED,F_OIL_LEVEL,F_MILEAGE,F_ALARM_DATA,F_GPS_TIME,F_RECV_TIME,F_ACC_STATUS,F_IS_REAL_LOCATION,F_TERMINAL_ID,F_ENTERPRISE_CODE,F_GPSENCRYPT,F_GPSSTATE,F_ISPASSUP,F_ALARM_DATA1,F_AREASN) VALUES (");
					SqlPackage(GpsSql, gps,Sqls);
				}

			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse Gps error", e);
			}
		}
		return Sqls;
	}

	private void SqlPackage(StringBuffer GpsSql, Gps gps,List<String> Sqls) {

		GpsSql.append(gps.getLocationID()).append(",").append(gps.getVehicleID()).append(",").append(gps.getLatitude())
				.append(",").append(gps.getLongitude()).append(",")
				.append((gps.getHigh() > 32767) ? (gps.getHigh() - 65536) : gps.getHigh()).append(",")
				.append((gps.getSpeed() > 32767) ? (gps.getSpeed() - 65536) : gps.getSpeed()).append(",")
				.append((gps.getDirection() > 32767) ? (gps.getDirection() - 65536) : gps.getDirection()).append(",")
				.append(gps.getAlarmCount()).append(",");
		if (gps.hasDrSpeed()) {
			GpsSql.append((gps.getDrSpeed() > 32767) ? (gps.getDrSpeed() - 65536) : gps.getDrSpeed());
		} else {
			GpsSql.append("null");
		}
		GpsSql.append(",");
		if (gps.hasOilLevel()) {
			GpsSql.append((gps.getOilLevel() > 32767) ? (gps.getOilLevel() - 65536) : gps.getOilLevel());
		} else {
			GpsSql.append("null");
		}
		GpsSql.append(",");
		if (gps.hasMileage()) {
			GpsSql.append(gps.getMileage());
		} else {
			GpsSql.append("null");
		}
		GpsSql.append(",");
		String S_Gpsdate = DateUtil.getStrTime(gps.getGpsDate(), "yyyy-MM-dd HH:mm:ss");
		String S_receiveD = DateUtil.getStrTime(gps.getReciveDate(), "yyyy-MM-dd HH:mm:ss");
		GpsSql.append(gps.getAlarmData()).append(",'").append(S_Gpsdate).append("','").append(S_receiveD).append("',");
		if (gps.getACC()) {
			GpsSql.append(1);
		} else {
			GpsSql.append(0);
		}
		GpsSql.append(",");
		if (gps.getIsRealLocation()) {
			GpsSql.append(1);
		} else {
			GpsSql.append(0);
		}
		GpsSql.append(",");

		GpsSql.append(gps.getTerminalID()).append(",'").append(gps.getEnterpriseCode()).append("',");
		if (gps.getGpsEncrypt()) {
			GpsSql.append(1);
		} else {
			GpsSql.append(0);
		}
		GpsSql.append(",");
		GpsSql.append(gps.getGpsState()).append(",");
		if (gps.getIsPassup()) {
			GpsSql.append(1);
		} else {
			GpsSql.append(0);
		}
		GpsSql.append(",").append(gps.getAlarmData1()).append(",'").append(gps.getArea()).append("')");
		Sqls.add(GpsSql.toString());
	}
}