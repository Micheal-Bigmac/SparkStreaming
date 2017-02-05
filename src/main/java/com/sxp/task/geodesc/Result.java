package com.sxp.task.geodesc;

import com.alibaba.fastjson.JSON;
import net.sf.json.JSONSerializer;
import net.sf.json.JsonConfig;
import net.sf.json.util.PropertyFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Result {
	private static final Logger LOG = LoggerFactory.getLogger(Result.class);
	private static JsonConfig JSONCONFIG = new JsonConfig();
	static {
		// 实现属性过滤器接口并重写apply()方法
		PropertyFilter pf = new PropertyFilter() {

			@Override
			// 返回true则跳过此属性，返回false则正常转换
			public boolean apply(Object source, String name, Object value) {
				if (value == null || String.valueOf(value).equals("")) {
					return true;
				}
				return false;
			}
		};
		// 将过滤器放入json-config中
		JSONCONFIG.setJsonPropertyFilter(pf);
	}
	private Location location;
	private String formatted_address;
	private String business;
	private AddressComponent addressComponent;
	private String cityCode;
	private List<Poi> pois;

	public Location getLocation() {
		return location;
	}

	public void setLocation(Location location) {
		this.location = location;
	}

	public String getFormatted_address() {
		return formatted_address;
	}

	public void setFormatted_address(String formatted_address) {
		this.formatted_address = formatted_address;
	}

	public String getBusiness() {
		return business;
	}

	public void setBusiness(String business) {
		this.business = business;
	}

	public AddressComponent getAddressComponent() {
		return addressComponent;
	}

	public void setAddressComponent(AddressComponent addressComponent) {
		this.addressComponent = addressComponent;
	}

	public String getCityCode() {
		return cityCode;
	}

	public void setCityCode(String cityCode) {
		this.cityCode = cityCode;
	}

	public List<Poi> getPois() {
		return pois;
	}

	public void setPois(List<Poi> pois) {
		this.pois = pois;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.getAddressComponent().getProvince() + ">");
		if (!this.getAddressComponent().getProvince().equals(this.getAddressComponent().getCity())) {
			sb.append(this.getAddressComponent().getCity() + ">");
		}
		sb.append(this.getAddressComponent().getDistrict() + ">");
		if (this.getPois().size() > 0) {
			sb.append(this.getPois().get(0).getAddr());
			sb.append(this.getPois().get(0).getName());
			sb.append(getDirection(this.getPois().get(0).getDirection()));
			sb.append(this.getPois().get(0).getDistance() + "米");
		}
		return sb.toString();
	}

	public String toJson() {
		return JSONSerializer.toJSON(this, JSONCONFIG).toString();
	}

	public static Result fromJson(String json) {
		Result r = null;
		try {
			if (json != null && !"".equals(json)) {
				r = JSON.parseObject(json, Result.class);
			}
		} catch (Exception e) {
			LOG.warn("parse geo result json error!", e);
		}
		return r;
	}

	private static String getDirection(int direction) {
		if (direction == 0)
			return "正北";
		if (direction == 45)
			return "东北";
		if (direction == 90)
			return "正东";
		if (direction > 0 && direction < 90)
			return "北偏东";
		if (direction == 135)
			return "东南";
		if (direction == 180)
			return "正南";
		if (direction > 90 && direction < 180)
			return "南偏东";
		if (direction == 225)
			return "西南";
		if (direction == 270)
			return "正西";
		if (direction > 180 && direction < 270)
			return "南偏西";
		if (direction == 315)
			return "西北";
		if (direction > 270 && direction < 360)
			return "北偏西";
		return "";
	}

}
