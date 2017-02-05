/**
 * @ClassName 
 * @Description 
 * @author 李小辉
 * @company 上海势航网络科技有限公司
 * @date 2016年11月19日
 */
package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.hbase.util.DateUtil;
import com.hsae.rdbms.db2.DB2ExcutorQuenue;
import com.sxp.task.protobuf.generated.DeviceRegInfo.DeviceReg;
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

public class DeviceRegDB2Bolt_2 extends BaseRichBolt {

	private static final Logger LOG = LoggerFactory.getLogger(DeviceRegDB2Bolt_2.class);
	public static final String TABLE_NAME_1 = "T_TERMINAL_REGISTER";
	public static final String TABLE_NAME_2 = "T_TERMINAL";
	public static final String TABLE_NAME_3 = "T_VEHICLE";
	private static Connection connection = null;

	public void prepare(Map stormConf) {
		connection = DB2ExcutorQuenue.isExstConnection(connection);
	}

	public void execute(Tuple input) {
		List<byte[]> dataList = (List<byte[]>) input.getValue(0);
		List<String> sqls=null;
		long start = 0;
		try {
			sqls = buildPreparedSqlAndValuesList(dataList);
			start = System.currentTimeMillis();
			if (sqls == null || sqls.size() == 0) {
				LOG.warn("no preparedSqlAndValuesList can be built from " + dataList.size() + " dataList.");
			} else {
				DB2ExcutorQuenue.excuteBatch(sqls,connection);
			}
		} catch (InvalidProtocolBufferException e) {
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
		}
	}

	protected List<String> buildPreparedSqlAndValuesList(List<byte[]> gpsList) throws InvalidProtocolBufferException {
		StringBuffer update1=null;
		StringBuffer update2=null;
		StringBuffer update3=null;
		List<String> sqls=new ArrayList<String>();
		for (int i = 0; i < gpsList.size(); i++) {
			DeviceReg d = DeviceReg.parseFrom(gpsList.get(i));
			update1=new StringBuffer();
			update1.append("update T_TERMINAL_REGISTER set F_AUTH_CODE='"+d.getAuthCode()+"',");
			update1.append("F_REGISTER_DATE='").append(DateUtil.getStrTime(d.getRegisterDate(), "yyyy-MM-dd HH:mm:ss")).append("',F_IS_LOGOUT=0,");
			String ipAddress = d.getIpAddress();
			if(ipAddress!=null){
				update1.append("F_IP_ADDRESS='").append(ipAddress+"',");
			}
			update1.append("F_PORT=").append(d.getPort()).append(",F_UPDATE_DATE='").append(DateUtil.getStrTime(d.getUpdateTime(), "yyyy-MM-dd HH:mm:ss")).append("'");
			update1.append(" where F_VEHICLE_ID=").append(d.getVehicleID());
			sqls.add(update1.toString());
			
			update2=new StringBuffer();
			update2.append("UPDATE T_TERMINAL set ");
			String provinceID = d.getProvinceID();
			if(provinceID!=null){
				update2.append("F_PROVINCE='").append(provinceID).append("',");
			}
			update2.append("F_EQUIPMENT_MODE='").append(d.getDeviceType()).append("',").append("F_PLATE_COLOR='").append(d.getPlateColor()).append("',");
			update2.append("F_MOBILE_CODE='").append(d.getSim()).append("',").append("F_ENTERPRISE_CODE='").append(d.getEnterpriseCode()).append("' ");
			update2.append("where F_ID = ").append(d.getDeviceID());
			sqls.add(update2.toString());
			
			update3=new StringBuffer();
			update3.append("UPDATE T_VEHICLE set  F_PLATE_COLOR= '").append(d.getPlateColor()).append("'").append(" where F_ID = ").append(d.getDeviceID());
			sqls.add(update3.toString());
		}
		return sqls;
	}


}
