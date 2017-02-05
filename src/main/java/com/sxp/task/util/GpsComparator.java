package com.sxp.task.util;

import com.sxp.task.protobuf.generated.GpsInfo;

/**
 * @ClassName: GpsComparator
 * @Description: 比较gps数据，先拿车辆id排序，再拿gps时间排序
 * @author: 韩欣宇
 * @company: 上海航盛实业有限公司
 * @date 2016年1月5日 下午5:06:15
 */
public class GpsComparator implements java.util.Comparator<GpsInfo.Gps> {

	public static GpsComparator GPS_COMPARATOR = new GpsComparator();

	@Override
	public int compare(GpsInfo.Gps o1, GpsInfo.Gps o2) {
		if (o1.getVehicleID() > o2.getVehicleID()) {
			return 1;
		} else if (o1.getVehicleID() < o2.getVehicleID()) {
			return -1;
		} else {
			if (o1.getGpsDate() > o2.getGpsDate()) {
				return 1;
			} else if (o1.getGpsDate() < o2.getGpsDate()) {
				return -1;
			} else {
				return 0;
			}
		}
	}
}
