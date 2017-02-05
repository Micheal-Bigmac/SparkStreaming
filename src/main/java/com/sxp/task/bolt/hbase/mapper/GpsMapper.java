package com.sxp.task.bolt.hbase.mapper;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.hbase.geography.Geocoder;
import com.hsae.hbase.table.History;
import com.hsae.hbase.table.History.GpsQ;
import com.sxp.hbase.table.HistoryExtend;
import com.sxp.task.geodesc.Poi;
import com.sxp.task.geodesc.Result;
import com.sxp.task.protobuf.generated.GpsInfo.*;
import com.sxp.task.util.GpsComparator;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
/**
 * @ClassName: GpsMapper
 * @Description: 写gps数据
 * @author: 韩欣宇
 * @company: 上海航盛实业有限公司
 * @date 2016年1月5日 下午5:29:35
 */
public class GpsMapper extends HbaseMapper {
	private static final Logger LOG = LoggerFactory.getLogger(GpsMapper.class);

	public Map<Long, Gps> vehicleId2LastGps = new HashMap<Long, Gps>();
	public Map<Long, Gps> vehicleIdAndLastGps = new HashMap<Long, Gps>();
	public Map<Long, Date> vehicleId2LastOffTime = new HashMap<Long, Date>();
	public Map<Long, Date> vehicleId2LastOnTime = new HashMap<Long, Date>();
	// 车辆所在道路名称
	public static final String HIGHWAY_ROAD = "高速公路";
	public static final String NATIONAL_ROAD = "国道";
	public static final String PROVINCE_ROAD = "省道";
	public static final String OTHER_ROAD = "其他道路";
	// 车辆所在道路类型
	public static final Integer HIGHWAY_ROAD_TYPE = 0;
	public static final Integer NATIONAL_ROAD_TYPE = 1;
	public static final Integer PROVINCE_ROAD_TYPE = 2;
	public static final Integer OTHER_ROAD_TYPE = 3;

	/*
	 * <p>Title: mutations</p> <p>Description: tuple的格式为：vehicleId,
	 * bytesListOfVehicle, resultsOfVehicle.</p>
	 * 
	 * @param tuple
	 * 
	 * @return
	 * 
	 * @see
	 * HbaseMapper#mutations(backtype.task.
	 * tuple.Tuple)
	 */
	@Override
	public List<Mutation> mutations(Tuple tuple) {
		Long vehicleId = (Long) tuple.getValue(0);
		List<byte[]> bytesList = (List<byte[]>) tuple.getValue(1);
		List<String> geoList = (List<String>) tuple.getValue(2);
		List<Mutation> mutations = new ArrayList<Mutation>();
		// gps及地理位置信息描述
		SortedMap<Gps, String> gps2Geo = new TreeMap<Gps, String>(GpsComparator.GPS_COMPARATOR);
		boolean hasCorrectGeographyDescription = bytesList.size() == geoList.size();
		for (int i = 0, size = bytesList.size(); i < size; i++) {
			byte[] bytes = bytesList.get(i);
			try {
				Gps gps = Gps.parseFrom(bytes);
				gps2Geo.put(gps, geoList.get(i));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("protobuf parse gps error! ", e);
			}
		}
		// 这个地方就要处理上一条gps与这一条gps的对比的一些逻辑了。比如报警聚合逻辑。
		for (Gps gps : gps2Geo.keySet()) {
			long vid = gps.getVehicleID();
			if (!vehicleId2LastGps.containsKey(vid)) {
				// TODO
				// 这里说明缓存中没有这个车的数据，就需要去redis里取这个车的最后一条的gps。后面如果不把数据存在redis里，那就不需要这个if了。
			}
			Gps oldGps = vehicleId2LastGps.get(vid);
			if (oldGps == null) {
				// 缓存中没有这个车的数据，这次加上
				vehicleId2LastGps.put(vid, gps);
			} else if (oldGps.getGpsDate() >= gps.getGpsDate()) {
				// 当前这条gps比缓存中的还要旧或一样，那就什么也不干。
			} else {
				if (oldGps.getACC() != gps.getACC()
						|| Math.abs(oldGps.getGpsDate() - gps.getGpsDate()) > (5 * 60 * 1000)) {
					// TODO 生成一个新的行程。
					Date start = new Date(oldGps.getGpsDate());
					Date end = new Date(gps.getGpsDate());
					int intervalSeconds = (int) ((gps.getGpsDate() - oldGps.getGpsDate()) / 1000);
					String interval = formatSecends(intervalSeconds);
				}
				vehicleId2LastGps.put(vid, gps);
			}
		}
		// 写hbase库
		for (Gps gps : gps2Geo.keySet()) {
			Put p = new Put(
					History.buildRowKeyUsingReverseVehicleIdAndBCDDateTime(gps.getVehicleID(), gps.getGpsDate()));
			p.addColumn(History.CF_GPS, GpsQ.ID, Bytes.toBytes(gps.getLocationID()));
			double lat = gps.getLatitude() * 1.0 / HistoryExtend.LATITUDE_ZOOM;
			double lng = gps.getLongitude() * 1.0 / HistoryExtend.LONGITUDE_ZOOM;
			double[] mlngLat = Geocoder.transformLngLat(lng, lat);
			long accIntervalTime = getAccIntervalTime(gps);
			// System.out.println(accIntervalTime);
			p.addColumn(History.CF_GPS, GpsQ.PLATE_CODE, Bytes.toBytes(gps.getVehicleName()));
			p.addColumn(History.CF_GPS, GpsQ.SIGNAL_STRENGTH, Bytes.toBytes(gps.getSignalStrength()));
			p.addColumn(History.CF_GPS, GpsQ.GNSSSATELLITENUMBER, Bytes.toBytes(gps.getGNSSSatelliteNumber()));
			p.addColumn(History.CF_GPS, GpsQ.LATITUDE, Bytes.toBytes(lat));
			p.addColumn(History.CF_GPS, GpsQ.LONGITUDE, Bytes.toBytes(lng));
			p.addColumn(History.CF_GPS, GpsQ.MARS_LATITUDE, Bytes.toBytes(mlngLat[1]));
			p.addColumn(History.CF_GPS, GpsQ.MARS_LONGITUDE, Bytes.toBytes(mlngLat[0]));
			p.addColumn(History.CF_GPS, GpsQ.HIGH, Bytes.toBytes((short) gps.getHigh()));
			p.addColumn(History.CF_GPS, GpsQ.SPEED,
					Bytes.toBytes((float) (gps.getSpeed() * 1.0 / HistoryExtend.SPEED_ZOOM)));
			p.addColumn(History.CF_GPS, GpsQ.DIRECTION, Bytes.toBytes((short) gps.getDirection()));
			p.addColumn(History.CF_GPS, GpsQ.ALARM_COUNT, Bytes.toBytes((short) gps.getAlarmCount()));
			p.addColumn(History.CF_GPS, GpsQ.RECORDER_SPEED,
					Bytes.toBytes((float) (gps.getDrSpeed() * 1.0 / HistoryExtend.RECORDER_SPEED_ZOOM)));
			p.addColumn(History.CF_GPS, GpsQ.OIL_LEVEL,
					Bytes.toBytes((float) (gps.getOilLevel() * 1.0 / HistoryExtend.OIL_LEVEL_ZOOM)));
			p.addColumn(History.CF_GPS, GpsQ.MILEAGE,
					Bytes.toBytes(((gps.getMileage() < 0) ? (gps.getMileage() + Math.pow(2, 32)) : gps.getMileage())* 1.0 / HistoryExtend.MILEAGE_ZOOM));
			p.addColumn(History.CF_GPS, GpsQ.ALARM_DATA,
					Bytes.add(Bytes.toBytes(gps.getAlarmData1()), Bytes.toBytes(gps.getAlarmData())));
			p.addColumn(History.CF_GPS, GpsQ.RECV_TIME, Bytes.toBytes(gps.getReciveDate()));
			p.addColumn(History.CF_GPS, GpsQ.ACC_STATUS, Bytes.toBytes(gps.getACC()));
			p.addColumn(History.CF_GPS, GpsQ.IS_LOCATION_FIXED, Bytes.toBytes(gps.getIsRealLocation()));
			p.addColumn(History.CF_GPS, GpsQ.GPS_ENCRYPT, Bytes.toBytes(gps.getGpsEncrypt()));
			p.addColumn(History.CF_GPS, GpsQ.GPS_STATE, Bytes.toBytes(gps.getGpsState()));
			p.addColumn(History.CF_GPS, GpsQ.IS_PASSUP, Bytes.toBytes(gps.getIsPassup()));
			p.addColumn(History.CF_GPS, GpsQ.ADMINISTRATIVE_CODING, Bytes.toBytes(gps.getArea()));
			if (hasCorrectGeographyDescription) {
				String geo = gps2Geo.get(gps);
				Result r = Result.fromJson(geo);
				// 判断位置信息里面addr字段或name字段里面是否以"高速公路"或"国道"或"省道"结尾，并写入hbase
				if (r != null && r.getPois().get(0) != null) {
					Poi position = r.getPois().get(0);
					if (position.getAddr()!=null && position.getName()!=null) {
						if (position.getAddr().endsWith(HIGHWAY_ROAD)) {
							p.addColumn(History.CF_GPS, GpsQ.ROAD_NAME, Bytes.toBytes(position.getAddr()));
							p.addColumn(History.CF_GPS, GpsQ.ROAD_TYPE, Bytes.toBytes(HIGHWAY_ROAD_TYPE));
						} else if (position.getName().endsWith(HIGHWAY_ROAD)) {
							p.addColumn(History.CF_GPS, GpsQ.ROAD_NAME, Bytes.toBytes(position.getName()));
							p.addColumn(History.CF_GPS, GpsQ.ROAD_TYPE, Bytes.toBytes(HIGHWAY_ROAD_TYPE));
						} else if (position.getAddr().endsWith(NATIONAL_ROAD)) {
							p.addColumn(History.CF_GPS, GpsQ.ROAD_NAME, Bytes.toBytes(position.getAddr()));
							p.addColumn(History.CF_GPS, GpsQ.ROAD_TYPE, Bytes.toBytes(NATIONAL_ROAD_TYPE));
						} else if (position.getName().endsWith(NATIONAL_ROAD)) {
							p.addColumn(History.CF_GPS, GpsQ.ROAD_NAME, Bytes.toBytes(position.getName()));
							p.addColumn(History.CF_GPS, GpsQ.ROAD_TYPE, Bytes.toBytes(NATIONAL_ROAD_TYPE));
						} else if (position.getAddr().endsWith(PROVINCE_ROAD)) {
							p.addColumn(History.CF_GPS, GpsQ.ROAD_NAME, Bytes.toBytes(position.getAddr()));
							p.addColumn(History.CF_GPS, GpsQ.ROAD_TYPE, Bytes.toBytes(PROVINCE_ROAD_TYPE));
						} else if (position.getName().endsWith(PROVINCE_ROAD)) {
							p.addColumn(History.CF_GPS, GpsQ.ROAD_NAME, Bytes.toBytes(position.getName()));
							p.addColumn(History.CF_GPS, GpsQ.ROAD_TYPE, Bytes.toBytes(PROVINCE_ROAD_TYPE));
						} else {
							p.addColumn(History.CF_GPS, GpsQ.ROAD_NAME, Bytes.toBytes(OTHER_ROAD));
							p.addColumn(History.CF_GPS, GpsQ.ROAD_TYPE, Bytes.toBytes(OTHER_ROAD_TYPE));
						}
					}
				}
				if (r != null) {
					p.addColumn(History.CF_GPS, GpsQ.GEOGRAPHY_DESCRIPTION, Bytes.toBytes(r.toString()));
					p.addColumn(History.CF_GPS, GpsQ.GEOGRAPHY_DESCRIPTION_JSON, Bytes.toBytes(r.toJson()));
				} else {
					p.addColumn(History.CF_GPS, GpsQ.GEOGRAPHY_DESCRIPTION, Bytes.toBytes(""));
					p.addColumn(History.CF_GPS, GpsQ.GEOGRAPHY_DESCRIPTION_JSON, Bytes.toBytes(""));
				}
			}
			if (!gps.getACC()) {
				p.addColumn(History.CF_GPS, GpsQ.ACC_OFF_TIME, Bytes.toBytes(accIntervalTime));
			} else {
				p.addColumn(History.CF_GPS, GpsQ.ACC_ON_TIME, Bytes.toBytes(accIntervalTime));
			}
			//添加扩展之诊断发动机信息
			CFBEngine cfb = gps.getFBEngine();
			if(cfb != null){
				addColumn(p, GpsQ.CFB_CKNJ, cfb.getCkNj());
				addColumn(p, GpsQ.CFB_GZD2NJ, cfb.getGzd2Nj());
				addColumn(p, GpsQ.CFB_GZD2ZS, cfb.getGzd2Zs());
				addColumn(p, GpsQ.CFB_GZD3NJ, cfb.getGzd3Nj());
				addColumn(p, GpsQ.CFB_GZD3ZS, cfb.getGzd3Zs());
				addColumn(p, GpsQ.CFB_GZD4NJ, cfb.getGzd4Nj());
				addColumn(p, GpsQ.CFB_GZD4ZS, cfb.getGzd4Zs());
				addColumn(p, GpsQ.CFB_GZD5NJ, cfb.getGzd5Nj());
				addColumn(p, GpsQ.CFB_GZD5ZS, cfb.getGzd5Zs());
				addColumn(p, GpsQ.CFB_GZD6ZGZS, cfb.getGzd6Zgzs());
				addColumn(p, GpsQ.CFB_GZD7CZZS, cfb.getGzd7Czzs());
				addColumn(p, GpsQ.CFB_KZNJ, cfb.getKzNj());
				addColumn(p, GpsQ.CFB_KZZS, cfb.getKzZs());
				addColumn(p, GpsQ.CFB_TSQKP, cfb.getTsqkp());
				addColumn(p, GpsQ.CFB_ZDNJ, cfb.getZdNj());
				addColumn(p, GpsQ.CFB_ZDSDSEC, cfb.getZdSdsec());
				addColumn(p, GpsQ.CFB_ZDZS, cfb.getZdZs());
				addColumn(p, GpsQ.CFB_ZGNJ, cfb.getZgNj());
				addColumn(p, GpsQ.CFB_ZGZS, cfb.getZgZs());
				addColumn(p, GpsQ.CFB_ENGINEMARK, cfb.getEngineMark());
			}
			//扩展之诊断故障码信息
			List<CFBDiagnosis> cfbDiagnosis = gps.getFBDiagnosisItemsList();
			if(cfbDiagnosis != null && cfbDiagnosis.size() > 0){
				for(int i = 0; i < cfbDiagnosis.size(); i++){
					addColumn(p, GpsQ.getCFBQ(i, GpsQ.CFB_GZLY), cfbDiagnosis.get(i).getGzly());
					addColumn(p, GpsQ.getCFBQ(i, GpsQ.CFB_GZDM), cfbDiagnosis.get(i).getGzdm());
					addColumn(p, GpsQ.getCFBQ(i, GpsQ.CFB_GZMX), cfbDiagnosis.get(i).getGzmx());
					addColumn(p, GpsQ.getCFBQ(i, GpsQ.CFB_GZCS), cfbDiagnosis.get(i).getGzcs());
				}
			}
			//添加附加信息
			List<AppendItem> itemsList = gps.getAppendItemsList();
			for (AppendItem appendItem : itemsList) {
				p.addColumn(History.CF_GPS, GpsQ.getAppendQ(appendItem.getAppendID()), Bytes.toBytes(appendItem.getAppendValue()));
			}
			//环卫车信息
			CFBState cState = gps.getFBState();
			if(cState != null){
				addColumn(p, GpsQ.CFB_FBHWCBOOT, cState.getFbHwcBoot());
				addColumn(p, GpsQ.CFB_FBHWCWORK, cState.getFbHwcWork());
			}
			
			mutations.add(p);
		}
		return mutations;
	}

	/**
	 * 根据ACC状态计算车辆熄火时间或行驶时间
	 * 
	 * @param gps
	 * @return
	 */
	private long getAccIntervalTime(Gps gps) {

		long vid = gps.getVehicleID();
		Gps oldGps = vehicleIdAndLastGps.get(vid);
		if (oldGps == null) {
			// 缓存中没有这个车的数据，这次加上
			vehicleIdAndLastGps.put(vid, gps);
			if (!gps.getACC()) {
				vehicleId2LastOffTime.put(vid, new Date(gps.getGpsDate()));
				return 0;
			} else {
				vehicleId2LastOnTime.put(vid, new Date(gps.getGpsDate()));
				return 0;
			}
		}
		// 缓存中有这辆车的数据
		if (oldGps != null) {
			// ACC状态由"开"到"关"
			if (oldGps.getACC() && !gps.getACC()) {
				vehicleId2LastOffTime.put(vid, new Date(gps.getGpsDate()));
				vehicleIdAndLastGps.put(vid, gps);
				return 0;
				// ACC状态由"关"到"开"
			} else if (!oldGps.getACC() && gps.getACC()) {
				vehicleId2LastOnTime.put(vid, new Date(gps.getGpsDate()));
				vehicleIdAndLastGps.put(vid, gps);
				return 0;
			}
			// ACC状态持续为关的状态，记录熄火时间
			if (!oldGps.getACC() && !gps.getACC()) {
				// 车辆熄火持续时间
				long accOffIntervalTime = (int) ((gps.getGpsDate() - vehicleId2LastOffTime.get(vid).getTime()) / 1000);
				vehicleIdAndLastGps.put(vid, gps);
				return accOffIntervalTime;
				// ACC状态持续为开的状态，记录行驶时间
			} else if (oldGps.getACC() && gps.getACC()) {
				// 车辆行驶持续时间
				long accOnIntervalTime = (int) ((gps.getGpsDate() - vehicleId2LastOnTime.get(vid).getTime()) / 1000);
				vehicleIdAndLastGps.put(vid, gps);
				return accOnIntervalTime;
			}
		}
		return 0;
	}
	
	private void addColumn(Put put, byte[] qualifier, String value){
		if(value == null || value.equals(""))
			return;
		put.addColumn(History.CF_GPS, qualifier, Bytes.toBytes(value));
	}

	private String formatSecends(int s) {
		int sec = s % 60;
		int m = s / 60;
		int hour = m / 60;
		int min = m % 60;
		return hour + ":" + String.format("%02d", min) + ":" + String.format("%02d", sec);
	}

}
