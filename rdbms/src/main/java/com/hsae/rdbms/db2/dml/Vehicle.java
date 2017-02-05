/**  
 * @Title Vehicle.java
 * @Package com.hsae.rdbms.db2.dml
 * @Description TODO(用一句话描述该文件做什么)
 * @author 韩欣宇
 * @date: 2016年3月2日 下午1:01:09
 * @company 上海势航网络科技有限公司
 * @version V1.0  
 */
package com.hsae.rdbms.db2.dml;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.hsae.rdbms.db2.DB2Excutor;

/**
 * @ClassName Vehicle
 * @Description TODO(这里用一句话描述这个类的作用)
 * @author 韩欣宇
 * @company 上海势航网络科技有限公司
 * @date 2016年3月2日 下午1:01:09
 */
public class Vehicle {
	public static List<Long> getVehicleId(long vehicleId) throws SQLException {
		Connection connection = null;
		try {
			String sql = "SELECT F_VEHICLE_ID FROM T_FBDIAGNOSISENGINECUR WHERE F_VEHICLE_ID = "+vehicleId+"";
			List<Long> result = new ArrayList<Long>();
			connection = DB2Excutor.getConnection();
			PreparedStatement pstmt = connection.prepareStatement(sql);
			ResultSet resultSet = pstmt.executeQuery();
			while (resultSet.next()) {
				result.add(resultSet.getLong(1));
			}
			connection.commit();
			return result;
		} catch (SQLException e) {
			DB2Excutor.rollbackConn(connection, e);
			throw e;
		} finally {
			DB2Excutor.closeConn(connection);
		}
	}

	public static void main(String[] args) throws SQLException {
		List<Long> vehicleIdList = getVehicleId(4684662162894175l);
		
		StringBuilder builder = new StringBuilder();
		if(vehicleIdList.size() > 0){
			builder.append("UPDATE T_FBDIAGNOSISENGINECUR SET ");
		}
	}

}
