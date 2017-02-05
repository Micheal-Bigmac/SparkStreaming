/**  
 * @Title GeographyUtil.java
 * @Package com.hsae.hbase.geography
 * @Description TODO(用一句话描述该文件做什么)
 * @author 韩欣宇
 * @date: 2016年5月19日 上午10:58:30
 * @company 上海势航网络科技有限公司
 * @version V1.0  
 */
package com.sxp.hbase.geography;

import com.sxp.hbase.table.HistoryExtend;

/**
 * @ClassName GeographyUtil
 * @Description TODO(这里用一句话描述这个类的作用)
 * @author 韩欣宇
 * @company 上海势航网络科技有限公司
 * @date 2016年5月19日 上午10:58:30
 */
public class GeographyTransformUtil {
	public static double transformLatitude(int lat) {
		double d = lat / HistoryExtend.LATITUDE_ZOOM;
		while (d > 90) {
			d = d - 90;
		}
		while (d < -90) {
			d = d + 90;
		}
		return d;
	}

	public static double transformLongitude(int lng) {
		double d = lng / HistoryExtend.LONGITUDE_ZOOM;
		while (d > 180) {
			d = d - 180;
		}
		while (d < -180) {
			d = d + 180;
		}
		return d;
	}
}
