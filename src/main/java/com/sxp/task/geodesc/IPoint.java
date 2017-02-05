package com.sxp.task.geodesc;

/**
 * @ClassName: IPoint
 * @Description:
 * @author: 姊佹鏋�
 * @company: 涓婃捣鑸洓瀹炰笟鏈夐檺鍏徃
 * @date 2012-06-26 10:22:15 +0800
 */
public interface IPoint {

	/**
	 * @Title: getLatitude
	 * @Description: 鍙栫含搴�
	 * @param @return 璁惧畾鏂囦欢
	 * @return String 杩斿洖绫诲瀷
	 * @author: 姊佹鏋�
	 * @date 2012-06-26 10:23:36 +0800
	 * @throws
	 */
	public String getLatitude();

	/**
	 * @Title: setLatitude
	 * @Description: 璁剧疆绾害
	 * @param @param latitude 璁惧畾鏂囦欢
	 * @return void 杩斿洖绫诲瀷
	 * @author: 姊佹鏋�
	 * @date 2012-06-26 10:23:39 +0800
	 * @throws
	 */
	public void setLatitude(String latitude);

	/**
	 * @Title: getLongitude
	 * @Description: 鍙栫粡搴�
	 * @param @return 璁惧畾鏂囦欢
	 * @return String 杩斿洖绫诲瀷
	 * @author: 姊佹鏋�
	 * @date 2012-06-26 10:23:42 +0800
	 * @throws
	 */
	public String getLongitude();

	/**
	 * @Title: setLongitude
	 * @Description: 璁剧疆缁忓害
	 * @param @param longitude 璁惧畾鏂囦欢
	 * @return void 杩斿洖绫诲瀷
	 * @author: 姊佹鏋�
	 * @date 2012-06-26 10:23:44 +0800
	 * @throws
	 */
	public void setLongitude(String longitude);

	/**
	 * 鍘熷缁忓害
	 * 
	 * @param fromLongitude
	 */
	public void setFromLongitude(double fromLongitude);

	public double getFromLongitude();

	/**
	 * 鍘熷绾害
	 * 
	 * @param fromLongitude
	 */
	public void setFromLatitude(double fromLongitude);

	public double getFromLatitude();

	/**
	 * gps鍔犲瘑鏍囧織
	 * 
	 * @param gpsEncrypt
	 */
	public void setGpsEncrypt(String gpsEncrypt);

	public String getGpsEncrypt();
}
