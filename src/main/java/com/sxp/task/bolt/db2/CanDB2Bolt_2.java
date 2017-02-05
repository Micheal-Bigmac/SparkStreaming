package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.hbase.util.DateUtil;
import com.hsae.rdbms.db2.DB2ExcutorQuenue;
import com.sxp.task.protobuf.generated.CanInfo.Can;
import com.sxp.task.protobuf.generated.CanInfo.CanItem;
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

public class CanDB2Bolt_2 extends BaseRichBolt {
	private static final Logger LOG = LoggerFactory.getLogger(CanDB2Bolt.class);
	static String CAN_TABLE_NAME = "T_CAN_";
	static String TABLE_CAN_ERROR_NAME = "T_CAN_ERR";
	private static Connection connection = null;

	static String CAN_DETAILS_TABLE_NAME = "T_CAN_DETAIL_";
	static String TABLE_CAN_DETAIL_ERROR_NAME = "T_CAN_DETAIL_ERR";

	public void execute(Tuple input) {
		@SuppressWarnings("unchecked")
		List<byte[]> dataList = (List<byte[]>) input.getValue(0);
		long start = 0;
		List<String> preparedSqlAndValuesList = null;
		try {
			preparedSqlAndValuesList = buildPreparedSqlAndValuesList(dataList);
			start = System.currentTimeMillis();
			if (preparedSqlAndValuesList == null || preparedSqlAndValuesList.size() == 0) {
				LOG.warn("no preparedSqlAndValuesList can be built from " + dataList.size() + " dataList.");
			} else {
				DB2ExcutorQuenue.excuteBatch(preparedSqlAndValuesList,connection);
			}
		} catch (InvalidProtocolBufferException e) {
			LOG.error(this.getClass().getName() + ".execute error!");
		} catch (IllegalArgumentException e) {
			LOG.error(this.getClass().getName() + ".execute error!");
		} catch (SQLException e) {
//			if (preparedSqlAndValuesList !=null && preparedSqlAndValuesList.size() > 0)
//				_collector.emit("404", new Values(preparedSqlAndValuesList));
		} finally {
			LOG.warn("execute sql for " + dataList.size() + " records in " + ((System.currentTimeMillis() - start))
					+ "ms");
		}

	}

	public void prepare(Map stormConf) {
		connection = DB2ExcutorQuenue.isExstConnection(connection);
	}


	protected List<String> buildPreparedSqlAndValuesList(List<byte[]> canList) throws InvalidProtocolBufferException {
		List<String> sqlList = new ArrayList<String>();
		String canTime, CAN_TABLE_NAME_DATE, CAN_DETAILS_TABLE_NAME_DATE;
		int index = 0;
		boolean is_error = false;// 标识是否走error表分支
		StringBuffer can_insert = null;// can数据拼接sql:会出现动态表，所有每个循环一个insert
		StringBuffer can_error_inset = new StringBuffer();// can_error数据拼接sql：固定表,使用values(?,?),(?,?)提升性能
		can_error_inset.append(
				"INSERT INTO T_CAN_ERR(F_ID,F_LOCATION_ID,F_VEHICLE_ID,F_CAN_TIME,F_RECV_TIME,F_TERMINAL_ID,F_ENTERPRISE_CODE) VALUES  ");

		StringBuffer can_detail_insert = null;// can_details数据拼接
		StringBuffer can_detail_error_insert = new StringBuffer();// can_details数据拼接sql：固定表,使用values(?,?),(?,?)提升性能
		can_detail_error_insert.append(
				"INSERT INTO T_CAN_DETAIL_ERR(F_ID,F_CAN_ID,F_CAN_TYPE_ID,F_VALUE,F_CONTENT,F_ENTERPRISE_CODE) VALUES  ");

		StringBuffer can_temp = null, can_detail_temp = null;// can的values值：单条记录,can_detals的values值：多条记录,个记录之间用","隔开

		for (int i = 0, size = canList.size(); i < size; i++) {
			try {
				Can can = Can.parseFrom(canList.get(i));
				can_temp = new StringBuffer();
				// can_temp数据组装
				can_temp.append(String.format("(%d,%d,%d,'%s','%s',%d,'%s')", can.getID(), can.getLocationID(),
						can.getVehicleID(), DateUtil.getStrTime(can.getCanTime(), "yyyy-MM-dd HH:mm:ss"),
						DateUtil.getStrTime(can.getRecvTime(), "yyyy-MM-dd HH:mm:ss"), can.getTerminalID(),
						can.getEnterpriseCode()));
				// can_detail_temp数据组装
				for (CanItem c : can.getItemsList()) {
					can_detail_temp = new StringBuffer();
					can_detail_temp.append(String.format("(%d,%d,%d,%f,%s,'%s')", c.getID(), can.getID(),
							c.getCanTypeID(), c.hasCanValue() ? c.getCanValue() : null,
							c.hasCanContent() ? c.getCanContent() : null, can.getEnterpriseCode()));

					// 当前条不是最后一条数据是要在values后面追加","
					if (index < can.getItemsList().size() - 1) {
						can_detail_temp.append(",");
					}
					index++;
				}

				if (DateUtil.subtractOneDay() <= can.getCanTime() && DateUtil.addOneDay() >= can.getCanTime()) {
					// 动态组装表名
					canTime = DateUtil.getStrTime(can.getCanTime(), "yyyyMMdd");
					CAN_TABLE_NAME_DATE = CAN_TABLE_NAME + canTime;
					CAN_DETAILS_TABLE_NAME_DATE = CAN_DETAILS_TABLE_NAME + canTime;
					// 因为表名是动态的，所以每次循环重新new 一个can_insert
					can_insert = new StringBuffer();
					can_insert.append(String.format(
							"INSERT INTO %s (F_ID,F_LOCATION_ID,F_VEHICLE_ID,F_CAN_TIME,F_RECV_TIME,F_TERMINAL_ID,F_ENTERPRISE_CODE) VALUES ",
							CAN_TABLE_NAME_DATE));
					can_insert.append(can_temp);
					sqlList.add(can_insert.toString());
					// 因为表名是动态的，所以每次循环重新new 一个can_detail_insert
					can_detail_insert = new StringBuffer();
					can_detail_insert.append(String.format(
							"INSERT INTO %s (F_ID,F_CAN_ID,F_CAN_TYPE_ID,F_VALUE,F_CONTENT,F_ENTERPRISE_CODE) VALUES  ",
							CAN_DETAILS_TABLE_NAME_DATE));
					can_detail_insert.append(can_detail_temp);
					sqlList.add(can_detail_insert.toString());
				} else {
					is_error = true;
					if (can_temp.length() > 0) {
						can_error_inset.append(can_temp);
						can_error_inset.append(",");
					}
					if (can_detail_temp.length() > 0) {
						can_detail_error_insert.append(can_detail_temp);
						can_detail_error_insert.append(",");
					}
				}

			} catch (

			InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse Can error", e);
			}
		}
		if (is_error) {
			String str = can_error_inset.toString();
			str = str.substring(0, str.length() - 1);
			sqlList.add(str);
			String CanStr = can_detail_error_insert.toString();
			CanStr = CanStr.substring(0, CanStr.length() - 1);
			sqlList.add(CanStr);
		}
		return sqlList;
	}
}