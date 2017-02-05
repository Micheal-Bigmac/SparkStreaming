package com.hsae.rdbms.db2.dml;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.hsae.rdbms.db2.DB2Excutor;

/**
 * @ClassName VehicleConfig
 * @Description 根据type值返回相对应的车辆配置信息
 * @author wuguolin
 * @date 2016年4月19日 15:24:35
 */
public class VehicleConfig implements Serializable {

	private Connection connection = null;

	public VehicleConfig() {
		if (connection == null)
			initConnection();
	}

	private List<ConfigInfo> getConfig(int type) throws SQLException, ClassNotFoundException {
		String sql = "SELECT v.F_ID,mc.F_X_MIN,mc.F_X_SUBMIN,mc.F_X_INTERVAL,mc.F_X_SUBMAX, mc.F_X_MAX,mc.F_Y_MIN,mc.F_Y_SUBMIN,mc.F_Y_INTERVAL,mc.F_Y_SUBMAX, mc.F_Y_MAX FROM T_VEHICLE v LEFT JOIN T_VEHICLE_MODE m ON v.F_VEHICLE_MODE_ID = m.F_ID LEFT JOIN T_VEHICLE_MODE_CHART mc ON mc.F_MODE_ID = m.F_ID WHERE mc.F_CHART_ID = "
				+ type;
		List<ConfigInfo> confList = new ArrayList<ConfigInfo>();
		try {
			PreparedStatement prepareStatement = connection.prepareStatement(sql);
			ResultSet resultSet = prepareStatement.executeQuery();
			ConfigInfo info = null;
			while (resultSet.next()) {
				info = new ConfigInfo();
				info.vehicleID = resultSet.getLong(1);
				info.x_min = resultSet.getInt(2);
				info.x_submin = resultSet.getInt(3);
				info.x_interval = resultSet.getInt(4);
				info.x_submax = resultSet.getInt(5);
				info.x_max = resultSet.getInt(6);
				info.y_min = resultSet.getInt(7);
				info.y_submin = resultSet.getInt(8);
				info.y_interval = resultSet.getInt(9);
				info.y_submax = resultSet.getInt(10);
				info.y_max = resultSet.getInt(11);
				confList.add(info);
			}
			connection.commit();
			confList.add(defaultConfigMap(type));
			return confList;
		} catch (SQLException e) {
			DB2Excutor.rollbackConn(connection, e);
			throw e;
		}
	}

	// 返回所有查询类型数据
	public Map<Integer, List<ConfigInfo>> getConfigMap() {
		Map<Integer, List<ConfigInfo>> map = new HashMap<Integer, List<ConfigInfo>>();
		CHARTENUM[] values = CHARTENUM.values();
		int typeid = 0;
		for (CHARTENUM chartenum : values) {
			typeid = chartenum.ordinal();
			try {
				map.put(typeid, getConfig(typeid));
			} catch (ClassNotFoundException | SQLException e) {
				e.printStackTrace();
			}
		}

		return map;
	}

	/**
	 * 默认配置
	 * 
	 * @return
	 */
	private ConfigInfo defaultConfigMap(int typeId) {
		ConfigInfo info = new ConfigInfo();
		info.vehicleID = -100;
		switch (typeId) {
		case 0:
			// 0:车速统计默认配置
			info.x_min = 10;
			info.x_submin = 10;
			info.x_interval = 10;
			info.x_submax = 100;
			info.x_max = 150;
			info.y_min = 0;
			info.y_submin = 100;
			info.y_interval = 100;
			break;
		case 1:
			// 1:发动机转速统计
			info.x_min = 700;
			info.x_submin = 700;
			info.x_interval = 200;
			info.x_submax = 2500;
			info.x_max = 2500;
			info.y_min = 0;
			info.y_submin = 100;
			info.y_interval = 100;
			break;
		case 2:
			// 2:档位统计
			info.x_min = -1;
			info.x_submin = 0;
			info.x_interval = 1;
			info.x_submax = 12;
			info.x_max = 12;
			info.y_min = 0;
			info.y_submin = 100;
			info.y_interval = 100;
			break;
		case 3:
			// 3:油门统计
			info.x_min = 10;
			info.x_submin = 10;
			info.x_interval = 10;
			info.x_submax = 100;
			info.x_max = 100;
			info.y_min = 0;
			info.y_submin = 100;
			info.y_interval = 100;
			break;
		case 5:
			// 5: 转速-车速-档位统计图
			info.x_min = 600;
			info.x_submin = 600;
			info.x_interval = 100;
			info.x_submax = 1700;
			info.x_max = 1700;
			info.y_min = 0;
			info.y_submin = 4;
			info.y_interval = 4;
			info.y_submax = 104;
			info.y_max = 104;
			break;
		case 6:
			// 6:转速-扭矩统计图
			info.x_min = 600;
			info.x_submin = 600;
			info.x_interval = 100;
			info.x_submax = 1800;
			info.x_max = 1800;
			info.y_min = 0;
			info.y_submin = 6;
			info.y_interval = 6;
			info.y_submax = 104;
			info.y_max = 104;
			break;
		case 7:
			// 7:瞬时油耗
			info.x_min = 5;
			info.x_submin = 15;
			info.x_interval = 10;
			info.x_submax = 95;
			info.x_max = 95;
			info.y_min = 0;
			info.y_submin = 50;
			info.y_interval = 50;
			info.y_submax = 300;
			info.y_max = 300;
			break;
		}
		return info;
	}

	// 返回所有svt80设备型号的车辆
	public List<Long> getVehicleMode() {
		List<Long> vehcileMode = new ArrayList<Long>();
		try {
			vehcileMode = getVehicleModeByMode("SVT80");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return vehcileMode;
	}

	/**
	 * 获取所有符合条件的上下线信息
	 * 
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public Map<Long, List<OnLineInfo>> getOnLine() {
		Map<Long, List<OnLineInfo>> online_map = new HashMap<Long, List<OnLineInfo>>();
		List<OnLineInfo> on_line_list;
		Date time = new Date();
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		String current_time = dateFormat.format(time);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(time);
		// 前一天
		calendar.add(Calendar.DAY_OF_MONTH, -1);
		Date startTime = calendar.getTime();
		String last_time = dateFormat.format(startTime);
		// 前2天
		calendar.add(Calendar.DAY_OF_MONTH, -1);
		Date last_start = calendar.getTime();
		String last_l_time = dateFormat.format(last_start);
		String sql = String.format(
				"select F_VEHICLE_ID,F_ONLINE_TIME,F_OFF_TIME from T_VEHICLE_ONLINE_LOG  WHERE F_ONLINE_TIME>='%s 0:00:00' and F_OFF_TIME>='%s 0:00:00' and  F_OFF_TIME<'%s 0:00:00'",
				last_l_time, last_time, current_time);
		try {

			PreparedStatement prepareStatement = connection.prepareStatement(sql);
			ResultSet resultSet = prepareStatement.executeQuery();
			OnLineInfo info = null;

			while (resultSet.next()) {
				info = new OnLineInfo();
				info.setVehicleID(resultSet.getLong(1));
				info.set_online(resultSet.getDate(2));
				info.set_offline(resultSet.getDate(3));
				if (online_map.containsKey(info.getVehicleID())) {
					on_line_list = online_map.get(info.getVehicleID());
					on_line_list.add(info);
				} else {
					on_line_list = new ArrayList<>();
					on_line_list.add(info);
				}
				online_map.put(info.getVehicleID(), on_line_list);
			}

		} catch (SQLException e) {
			try {
				throw e;
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			DB2Excutor.rollbackConn(connection, e);
		}
		return online_map;
	}

	// 根据设备类型返回设备
	private List<Long> getVehicleModeByMode(String mode) throws ClassNotFoundException, SQLException {
		String sql = "select  F_VEHICLE_ID  from T_TERMINAL where F_EQUIPMENT_MODE='" + mode + "'";
		List<Long> vehcileMode = new ArrayList<Long>();
		try {
			PreparedStatement prepareStatement = connection.prepareStatement(sql);
			ResultSet resultSet = prepareStatement.executeQuery();
			while (resultSet.next()) {
				vehcileMode.add(resultSet.getLong(1));
			}
		} catch (SQLException e) {
			DB2Excutor.rollbackConn(connection, e);
			throw e;
		}
		return vehcileMode;
	}

	private void initConnection() {
		try {
			Class.forName("com.ibm.db2.jcc.DB2Driver");
			// online
//			connection = DriverManager.getConnection("jdbc:db2://10.10.13.111:50000/CVNAVIDB", "db2inst1", "hsae@1706");

			 connection=DriverManager.getConnection("jdbc:db2://10.10.10.231:50000/hsae_db","db2inst1", "hsae@1706");
		} catch (ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// 第二步：得到连接

	}

	public void closed() {
		if (connection != null)
			DB2Excutor.closeConn(connection);
	}

	public static class ConfigInfo implements Serializable {
		private long vehicleID;
		private int x_min;
		private int x_submin;
		private int x_interval;
		private int x_submax;
		private int x_max;
		private int y_min;
		private int y_submin;
		private int y_interval;
		private int y_submax;
		private int y_max;

		public long getVehicleID() {
			return vehicleID;
		}

		public void setVehicleID(long vehicleID) {
			this.vehicleID = vehicleID;
		}

		public int getX_min() {
			return x_min;
		}

		public void setX_min(int x_min) {
			this.x_min = x_min;
		}

		public int getX_submin() {
			return x_submin;
		}

		public void setX_submin(int x_subMin) {
			this.x_submin = x_subMin;
		}

		public int getX_interval() {
			return x_interval;
		}

		public void setX_interval(int x_interval) {
			this.x_interval = x_interval;
		}

		public int getX_submax() {
			return x_submax;
		}

		public void setX_submax(int x_submax) {
			this.x_submax = x_submax;
		}

		public int getX_max() {
			return x_max;
		}

		public void setX_max(int x_max) {
			this.x_max = x_max;
		}

		public int getY_min() {
			return y_min;
		}

		public void setY_min(int y_min) {
			this.y_min = y_min;
		}

		public int getY_submin() {
			return y_submin;
		}

		public void setY_submin(int y_subMin) {
			this.y_submin = y_subMin;
		}

		public int getY_interval() {
			return y_interval;
		}

		public void setY_interval(int y_interval) {
			this.y_interval = y_interval;
		}

		public int getY_submax() {
			return y_submax;
		}

		public void setY_submax(int y_submax) {
			this.y_submax = y_submax;
		}

		public int getY_max() {
			return y_max;
		}

		public void setY_max(int y_max) {
			this.y_max = y_max;
		}

		@Override
		public String toString() {
			return "ConfigInfo [vehicleID=" + vehicleID + ", x_min=" + x_min + ", x_submin=" + x_submin
					+ ", x_interval=" + x_interval + ", x_submax=" + x_submax + ", x_max=" + x_max + ", y_min=" + y_min
					+ ", y_submin=" + y_submin + ", y_interval=" + y_interval + ", y_submax=" + y_submax + ", y_max="
					+ y_max + "]";
		}

	}

	public enum CHARTENUM {
		SpeedStatistics, // 0 车速统计
		EngineSpeedStatistics, // 1 发动机转速统计
		TransmissionGearStatistics, // 2 档位统计
		AcceleratorStatistics, // 3 油门统计
		TurnSpeed, // 4 经济转速
		RPMSpeedGearStatistics, // 5 转速-车速-档位统计图
		RPMTorqueStatistics, // 6 转速-扭矩统计图
		REALTIMEOIL,//7瞬时油耗

	}

	public static class OnLineInfo implements Serializable, Comparable<OnLineInfo> {
		private long vehicleID;
		private Date _online;
		private Date _offline;

		public long getVehicleID() {
			return vehicleID;
		}

		public void setVehicleID(long vehicleID) {
			this.vehicleID = vehicleID;
		}

		public Date get_online() {
			return _online;
		}

		public void set_online(Date _online) {
			this._online = _online;
		}

		public Date get_offline() {
			return _offline;
		}

		public void set_offline(Date _offline) {
			this._offline = _offline;
		}

		@Override
		public int compareTo(OnLineInfo o) {
			return this.get_online().getTime() > o.get_online().getTime() ? -1 : 1;
		}

	}

	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		System.out.println(new VehicleConfig().getConfigMap());

	}
}
