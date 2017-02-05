/**  
 * @Title HistoryExtend.java
 * @Package com.hsae.hbase.table
 * @Description TODO(用一句话描述该文件做什么)
 * @author 韩欣宇
 * @date: 2016年3月4日 上午9:42:33
 * @company 上海势航网络科技有限公司
 * @version V1.0  
 */
package com.sxp.hbase.table;

import com.hsae.hbase.table.History;

/**
 * @ClassName HistoryExtend
 * @Description 扩展History表的一些相关值
 * @author 韩欣宇
 * @company 上海势航网络科技有限公司
 * @date 2016年3月4日 上午9:42:33
 */
public class HistoryExtend extends History {
	public static final double LATITUDE_ZOOM = 1000000;
	public static final double LONGITUDE_ZOOM = 1000000;
	public static final int SPEED_ZOOM = 10;
	public static final int RECORDER_SPEED_ZOOM = 10;
	public static final int OIL_LEVEL_ZOOM = 10;
	public static final int MILEAGE_ZOOM = 10;

}
