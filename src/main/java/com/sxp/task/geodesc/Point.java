package com.sxp.task.geodesc;

/**
 * @ClassName: Point
 * @Description:鍦板浘鐨勫潗鏍囩偣,杩欓噷杈圭殑缁忕含搴︽槸瀹為檯缁忕含搴︽斁澶�0^6
 * @author: 姊佹鏋�
 * @company: 涓婃捣鑸洓瀹炰笟鏈夐檺鍏徃
 * @date 2012-06-26 16:21:54 +0800
 */
public class Point implements IPoint {

	/**
	 * @Fields longitude :缁忓害,瀹為檯缁忕含搴︽斁澶�0^6
	 */
	private String longitude;

	/**
	 * @Fields latitude : 绾害,瀹為檯缁忕含搴︽斁澶�0^6
	 */
	private String latitude;

	private double fromLongitude;// 鍘熷缁忓害

	private double fromLatitude;// 鍘熷绾害

	/**
	 * @Fields gpsEncrypt : 鍔犲瘑鏍囧織
	 */
	private String gpsEncrypt;

	public Point() {
	}

	public Point(String longitude, String latitude) {
		this.longitude = longitude;
		this.latitude = latitude;
	}

	public Point(String longitude, String latitude, String gpsEncrypt) {
		this.longitude = longitude;
		this.latitude = latitude;
		this.gpsEncrypt = gpsEncrypt;
	}

	public Point(String longitude, String latitude, double fromLongitude, double fromLatitude) {
		this.longitude = longitude;
		this.latitude = latitude;
		this.fromLongitude = fromLongitude;
		this.fromLatitude = fromLatitude;
	}

	@Override
	public double getFromLongitude() {
		return fromLongitude / 1000000.0;
	}

	@Override
	public void setFromLongitude(double fromLongitude) {
		this.fromLongitude = fromLongitude;
	}

	@Override
	public double getFromLatitude() {
		return fromLatitude / 1000000.0;
	}

	@Override
	public void setFromLatitude(double fromLatitude) {
		this.fromLatitude = fromLatitude;
	}

	/**
	 * @return the latitude
	 */
	@Override
	public String getLatitude() {
		return latitude;
	}

	/**
	 * @param latitude
	 *            the latitude to set
	 */
	@Override
	public void setLatitude(String latitude) {
		this.latitude = latitude;
	}

	/**
	 * @return the longitude
	 */
	@Override
	public String getLongitude() {
		return longitude;
	}

	/**
	 * @param longitude
	 *            the longitude to set
	 */
	@Override
	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}

	@Override
	public void setGpsEncrypt(String gpsEncrypt) {
		this.gpsEncrypt = gpsEncrypt;
	}

	@Override
	public String getGpsEncrypt() {
		return gpsEncrypt;
	}
}
