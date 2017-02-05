package com.sxp.task.bolt;

import com.alibaba.fastjson.JSON;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sxp.hbase.geography.GeographyTransformUtil;
import com.sxp.task.geodesc.Response;
import com.sxp.task.geodesc.ResponseList;
import com.sxp.task.protobuf.generated.GpsInfo;
import org.apache.storm.tuple.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;

// TODO support more configuration options, for now we're defaulting to the hbase-*.xml files found on the classpath
public class GeographyDescriptionBolt extends BaseRichBolt {
	private static final Logger LOG = LoggerFactory.getLogger(GeographyDescriptionBolt.class);
	private static final boolean NEED_GEOGRAPHY_DESCRIPTION = true;
	private static final String SOURCE_FIELD_NAME = "source";
	private static final String GEO_FIELD_NAME = "withgeo";
	public static final String GROUPING_FIELD = "vehicleid";


	protected String mapDecoderUrl;
	protected String mapDecoderUid;
	protected String mapDecoderUrlParams;
    protected Properties config;
	public static final String CONFIG_KEY = "geography.description";

	public GeographyDescriptionBolt() {
	}

	public void prepare(Properties prop) {
        if(prop==null){
            throw new NullPointerException("properties is null");
        }else{
            config=prop;
            mapDecoderUrl=(String)prop.get("map.decoder.url");
            mapDecoderUid=(String)prop.get("map.decoder.uid");
		}
		mapDecoderUrlParams = "output=json&pois=1&uid=" + mapDecoderUid + "&points=";
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		List<byte[]> bytesList = (List<byte[]>) input.getValue(0);
		if (bytesList == null || bytesList.size() == 0) {
//			this.collector.ack(input);
			return;
		}
		try {
			StringBuilder sb = new StringBuilder(mapDecoderUrlParams);
			long start = System.currentTimeMillis();
			List<GpsInfo.Gps> gpss = new ArrayList<GpsInfo.Gps>();// 这个list与bytesList和result的size是一样的，并且index相同的元素是一一对应的
			for (byte[] bytes : bytesList) {
				GpsInfo.Gps gps = GpsInfo.Gps.parseFrom(bytes);
				gpss.add(gps);
				if (NEED_GEOGRAPHY_DESCRIPTION) {
					sb.append(GeographyTransformUtil.transformLongitude(gps.getLongitude())).append(",").append(GeographyTransformUtil.transformLatitude(gps.getLatitude()))
							.append(";");
				}
			}
			List<String> result = new ArrayList<String>();
			if (NEED_GEOGRAPHY_DESCRIPTION) {
//				result = this.getGeographyDescription(mapDecoderUrl, sb.toString());
                result=null;
			} else {
				// 暂时先不要位置信息解析功能
				for (int i = 0; i < bytesList.size(); i++) {
					result.add("暂时先不要位置信息解析功能");
				}
			}
//            Fields transFields=new Fields("VehicleId","Source","withGeo");
//            GpsMapper gpsMapper=new GpsMapper();
//            AlarmAggrBolt alarmAggrBolt=new AlarmAggrBolt();
			if (result != null && result.size() == bytesList.size()) {
				Map<Long, List<Values>> vehicleId2Values = new HashMap<Long, List<Values>>();
				for (int i = 0, size = gpss.size(); i < size; i++) {
					GpsInfo.Gps gps = gpss.get(i);
					long vehicleId = gps.getVehicleID();
					Values v = new Values(bytesList.get(i), result.get(i));
					if (!vehicleId2Values.containsKey(vehicleId)) {
						ArrayList<Values> value = new ArrayList<Values>();
						value.add(v);
						vehicleId2Values.put(vehicleId, value);
					} else {
						vehicleId2Values.get(vehicleId).add(v);
					}
				}
				for (long vehicleId : vehicleId2Values.keySet()) {
					List<Values> valuesOfVehicle = vehicleId2Values.get(vehicleId);
					List<byte[]> bytesListOfVehicle = new ArrayList<byte[]>();
					List<String> resultsOfVehicle = new ArrayList<String>();
					for (Values values : valuesOfVehicle) {
						bytesListOfVehicle.add((byte[]) values.get(0));
						resultsOfVehicle.add((String) values.get(1));
					}

                    Values transValue = new Values(vehicleId, bytesListOfVehicle, resultsOfVehicle);
                    this.results.addAll(transValue);
//					collector.emit(input, new Values(vehicleId, bytesListOfVehicle, resultsOfVehicle));

					LOG.debug("NEED_GEOGRAPHY_DESCRIPTION = " + NEED_GEOGRAPHY_DESCRIPTION + ", GeographyDescriptionBolt get " + resultsOfVehicle.size()
							+ " geography description in " + (System.currentTimeMillis() - start) + "ms.");
				}
			} else {
				Map<Long, List<Values>> vehicleId2Values = new HashMap<Long, List<Values>>();
				for (int i = 0, size = gpss.size(); i < size; i++) {
					GpsInfo.Gps gps = gpss.get(i);
					long vehicleId = gps.getVehicleID();
					String v_null="";
					Values v = new Values(bytesList.get(i), v_null);
					if (!vehicleId2Values.containsKey(vehicleId)) {
						List<Values> value = new ArrayList<Values>();
						value.add(v);
						vehicleId2Values.put(vehicleId, value);
					} else {
						vehicleId2Values.get(vehicleId).add(v);
					}
				}
				for (long vehicleId : vehicleId2Values.keySet()) {
					List<Values> valuesOfVehicle = vehicleId2Values.get(vehicleId);
					List<byte[]> bytesListOfVehicle = new ArrayList<byte[]>();
					List<String> resultsOfVehicle = new ArrayList<String>();
					for (Values values : valuesOfVehicle) {
						bytesListOfVehicle.add((byte[]) values.get(0));
						resultsOfVehicle.add((String) values.get(1));
					}
//					collector.emit(input, new Values(vehicleId, bytesListOfVehicle, resultsOfVehicle));
                    Values transValue = new Values(vehicleId, bytesListOfVehicle, resultsOfVehicle);
                    this.results.addAll(transValue);
					LOG.debug("NEED_GEOGRAPHY_DESCRIPTION = " + NEED_GEOGRAPHY_DESCRIPTION + ", GeographyDescriptionBolt get " + resultsOfVehicle.size()
							+ " geography description in " + (System.currentTimeMillis() - start) + "ms.");
				}
//				LOG.warn("GeographyDescriptionBolt get 0 geography description of " + bytesList.size() + " Gps' in " + (System.currentTimeMillis() - start) + "ms.");
			}
		} catch (InvalidProtocolBufferException e) {
			LOG.error("protobuf parse gps error! ", e);
		} catch (IOException e) {
			LOG.error("get geography description error! ", e);
		}
	}

	public List<String> getGeographyDescription(String mapDecoderUrl, String paramString) throws IOException {
		List<String> result = new ArrayList<String>();
		String jsonStr = getHttpConnectResponseWithGet(mapDecoderUrl, paramString);
		ResponseList responseList = JSON.parseObject(jsonStr, ResponseList.class);
		if (responseList.getStatus() == 0) {
			for (Response r : responseList.getGeocoderResponses()) {
				if (r.getStatus() == 0) {
					result.add(r.getResult().toJson());
				} else {
					result.add("");
				}
			}
		}
		return result;
	}

	String getHttpConnectResponseWithGet(String mapDecoderUrl, String paramString) throws IOException {
		URL url = new URL(mapDecoderUrl);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("POST");
		conn.setDoOutput(true);// 是否输入参数
		conn.getOutputStream().write(paramString.getBytes());// 输入参数
		InputStream inStream = conn.getInputStream();
		String result = new String(readInputStream(inStream), "UTF-8");
		inStream.close();
		conn.disconnect();
		return result;
	}

	private byte[] readInputStream(InputStream inStream) throws IOException {
		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		byte[] buffer = new byte[1024];
		int len = 0;
		while ((len = inStream.read(buffer)) != -1) {
			outStream.write(buffer, 0, len);
		}
		byte[] data = outStream.toByteArray();
		outStream.close();
		inStream.close();
		return data;
	}
}
