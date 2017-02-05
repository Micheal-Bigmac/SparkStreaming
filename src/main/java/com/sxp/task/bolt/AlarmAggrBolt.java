package com.sxp.task.bolt;

import com.google.protobuf.InvalidProtocolBufferException;
import com.sxp.hbase.bean.AlarmAggrBean;
import com.sxp.hbase.bean.AreaInOutAlarmAggrBean;
import com.sxp.hbase.bean.OverSpeedAlarmAggrBean;
import com.sxp.hbase.bean.OverTimeAlarmAggrBean;
import com.sxp.hbase.table.HistoryExtend;
import com.sxp.task.protobuf.generated.GpsInfo;
import com.sxp.task.util.GpsComparator;
import org.apache.storm.tuple.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class AlarmAggrBolt extends BaseRichBolt {
	private static final long serialVersionUID = -6222029318353813732L;
	private static final Logger LOG = LoggerFactory.getLogger(AlarmAggrBolt.class);
	private static final String SOURCE_FIELD_NAME = "source";
	private static final String ALARMAGGR = "AlarmAggr";
	private static final String AREAINOUT = "AreaInOut";
	private static final String OVERSPEED = "OverSpeed";
	private static final String OVERTIME = "OverTime";

	private HashMap<Long, GpsInfo.Gps> vehicleId2LastGps = new HashMap<Long, GpsInfo.Gps>();
	private Map<Long, ArrayList<AlarmAggrBean>> vehicleId2AlarmAggrBeanList = new HashMap<Long, ArrayList<AlarmAggrBean>>();
	private Map<Long, ArrayList<AreaInOutAlarmAggrBean>> vehicleId2AreaInOutAlarmAggrBeanList = new HashMap<Long, ArrayList<AreaInOutAlarmAggrBean>>();
	private Map<Long, ArrayList<OverSpeedAlarmAggrBean>> vehicleId2OverSpeedAlarmAggrBeanList = new HashMap<Long, ArrayList<OverSpeedAlarmAggrBean>>();
	private Map<Long, ArrayList<OverTimeAlarmAggrBean>> vehicleId2OverTimeAlarmAggrBeanList = new HashMap<Long, ArrayList<OverTimeAlarmAggrBean>>();

	public void prepare(Map map) {
		this.toString();
	}

	public void execute(Tuple input) {
		long vehicleId = (long) input.getValue(0);
		List<byte[]> bytesList = (List<byte[]>) input.getValue(1);
		if (bytesList == null || bytesList.size() == 0) {
//			this.collector.ack(input);
			return;
		}
		try {
			TreeSet<GpsInfo.Gps> gpsSet = new TreeSet<>(GpsComparator.GPS_COMPARATOR);
			for (byte[] bytes : bytesList) {
				GpsInfo.Gps gps = GpsInfo.Gps.parseFrom(bytes);
				gpsSet.add(gps);
			}
			// 处理基础报警（不是像区域相关的报警那样需要区分区域id的）
			ArrayList<AlarmAggrBean> needAggrAlarmList = new ArrayList<>();
			// 处理进出区域报警
			ArrayList<AreaInOutAlarmAggrBean> needAggrAreaInoutList = new ArrayList<>();
			// 处理超速报警
			ArrayList<OverSpeedAlarmAggrBean> needAggrOverSpeedList = new ArrayList<>();
			// 处理超时/不足报警
			ArrayList<OverTimeAlarmAggrBean> needAggrOverTimeList = new ArrayList<>();
			for (GpsInfo.Gps currentGps : gpsSet) {
				GpsInfo.Gps lastGps = vehicleId2LastGps.get(vehicleId);
				vehicleId2LastGps.put(vehicleId, currentGps);
				// 之前没有gps信息，本次出现的所有报警，都是第一次报警
				//第一次报警，不做聚合，存入缓存中
				if (lastGps == null) {
					List<GpsInfo.AlarmItem> alarmItemsList = currentGps.getAlarmItemsList();
					for (GpsInfo.AlarmItem alarmItem : alarmItemsList) {
						AlarmAggrBean alarmAggrBean = new AlarmAggrBean(currentGps.getVehicleID(), alarmItem.getAlarmType());
						fillAlarmAggrBean(alarmAggrBean, currentGps);
						addToVehicleId2AlarmAggrBeanList(vehicleId, alarmAggrBean);
					}
					List<GpsInfo.AreaInout> areaInoutList = currentGps.getAreaInoutList();
					for (GpsInfo.AreaInout areaInout : areaInoutList) {
						AreaInOutAlarmAggrBean areaInOutAlarmAggrBean = new AreaInOutAlarmAggrBean(currentGps.getVehicleID(), areaInout.getAlarmTypeID());
						fillAreaInOutAlarmAggrBean(areaInOutAlarmAggrBean, currentGps, areaInout);
						addToVehicleId2AreaInOutAlarmAggrBeanList(vehicleId, areaInOutAlarmAggrBean);
					}
					List<GpsInfo.OverSpeed> overSpeedList = currentGps.getOverSpeedList();
					for (GpsInfo.OverSpeed overSpeed : overSpeedList) {
						OverSpeedAlarmAggrBean overSpeedAlarmAggrBean = new OverSpeedAlarmAggrBean(currentGps.getVehicleID(), overSpeed.getAlarmTypeID());
						fillOverSpeedAlarmAggrBean(overSpeedAlarmAggrBean, currentGps, overSpeed);
						addToVehicleId2OverSpeedAlarmAggrBeanList(vehicleId, overSpeedAlarmAggrBean);
					}
					List<GpsInfo.OverTime> overTimeList = currentGps.getOverTimeList();
					for (GpsInfo.OverTime overTime : overTimeList) {
						OverTimeAlarmAggrBean overTimeAlarmAggrBean = new OverTimeAlarmAggrBean(currentGps.getVehicleID(), overTime.getAlarmTypeID());
						fillOverTimeAlarmAggrBean(overTimeAlarmAggrBean, currentGps, overTime);
						addToVehicleId2OverTimeAlarmAggrBeanList(vehicleId, overTimeAlarmAggrBean);
					}
				} else {
					// 处理基础报警（不是像区域相关的报警那样需要区分区域id的）
					List<AlarmAggrBean> needAggrAlarm = dealAlarmItem(vehicleId, currentGps, lastGps);
					needAggrAlarmList.addAll(needAggrAlarm);
					// 处理进出区域报警
					ArrayList<AreaInOutAlarmAggrBean> needAggrAreaInout = dealAreaInout(vehicleId, currentGps, lastGps);
					needAggrAreaInoutList.addAll(needAggrAreaInout);
					// 处理超速报警
					ArrayList<OverSpeedAlarmAggrBean> needAggrOverSpeed = dealOverSpeed(vehicleId, currentGps, lastGps);
					needAggrOverSpeedList.addAll(needAggrOverSpeed);
					// 处理超时/不足报警
					ArrayList<OverTimeAlarmAggrBean> needAggrOverTime = dealOverTime(vehicleId, currentGps, lastGps);
					needAggrOverTimeList.addAll(needAggrOverTime);
				}
			}
			{
				List<byte[]> alarmAggrBytesList = new ArrayList<>();
				for (AlarmAggrBean alarmAggrBean : needAggrAlarmList) {
					alarmAggrBytesList.add(alarmAggrBean.toByteArray());
				}
				if (alarmAggrBytesList.size() > 0) {
//					collector.emit(ALARMAGGR, input, new Values(alarmAggrBytesList));
				}
			}
			{
				List<byte[]> areaInOutBytesList = new ArrayList<>();
				for (AreaInOutAlarmAggrBean areaInOutAlarmAggrBean : needAggrAreaInoutList) {
					areaInOutBytesList.add(areaInOutAlarmAggrBean.toByteArray());
				}
				if (areaInOutBytesList.size() > 0) {
//					collector.emit(AREAINOUT, input, new Values(areaInOutBytesList));
				}
			}
			{
				List<byte[]> overSpeedBytesList = new ArrayList<>();
				for (OverSpeedAlarmAggrBean overSpeedAlarmAggrBean : needAggrOverSpeedList) {
					overSpeedBytesList.add(overSpeedAlarmAggrBean.toByteArray());
				}
				if (overSpeedBytesList.size() > 0) {
//					collector.emit(OVERSPEED, input, new Values(overSpeedBytesList));
				}
			}
			{
				List<byte[]> overTimeBytesList = new ArrayList<>();
				for (OverTimeAlarmAggrBean overTimeAlarmAggrBean : needAggrOverTimeList) {
					overTimeBytesList.add(overTimeAlarmAggrBean.toByteArray());
				}
				if (overTimeBytesList.size() > 0) {
//					collector.emit(OVERTIME, input, new Values(overTimeBytesList));
				}
			}
		} catch (InvalidProtocolBufferException e) {
			LOG.error("protobuf parse gps error! ", e);
		}
	}

//	@Override
//	public void declareOutputFields(OutputFieldsDeclarer declarer) {
//		// declarer.declare(new Fields(GROUPING_FIELD, SOURCE_FIELD_NAME, GEO_FIELD_NAME));
//		declarer.declareStream(ALARMAGGR, new Fields(SOURCE_FIELD_NAME));
//		declarer.declareStream(AREAINOUT, new Fields(SOURCE_FIELD_NAME));
//		declarer.declareStream(OVERSPEED, new Fields(SOURCE_FIELD_NAME));
//		declarer.declareStream(OVERTIME, new Fields(SOURCE_FIELD_NAME));
//	}

	/**
	 * @Title dealAlarmItem
	 * @Description 处理基础报警（不是像区域相关的报警那样需要区分区域id的）
	 * @param vehicleId
	 * @param currentGps
	 * @param lastGps
	 * @author 韩欣宇
	 * @return
	 * @date 2016年2月24日 下午4:35:00
	 */
	private List<AlarmAggrBean> dealAlarmItem(long vehicleId, GpsInfo.Gps currentGps, GpsInfo.Gps lastGps) {
		ArrayList<AlarmAggrBean> needToAggrAlarmAggrBeanList = new ArrayList<>();
		List<GpsInfo.AlarmItem> alarmItemsList = currentGps.getAlarmItemsList();
		if (!vehicleId2AlarmAggrBeanList.containsKey(vehicleId)) {
			vehicleId2AlarmAggrBeanList.put(vehicleId, new ArrayList<AlarmAggrBean>());
		}
		ArrayList<AlarmAggrBean> alarmAggrBeanList = vehicleId2AlarmAggrBeanList.get(vehicleId);
		// 本次没有出现过的报警，但是之前出现过的报警，都需要被聚合结束掉
		for (AlarmAggrBean alarmAggrBean : alarmAggrBeanList) {
			boolean appear = false;
			for (GpsInfo.AlarmItem alarmItem : alarmItemsList) {
				//之前出现过报警，现在也出现了
				if (alarmAggrBean.getAlarmTypeId() == alarmItem.getAlarmType()) {
					appear = true;
					break;
				}
			}
			//之前出现了报警，现在没有出现，加入到需要聚合的list中
			if (!appear) {
				needToAggrAlarmAggrBeanList.add(alarmAggrBean);
			}
		}
		//从缓存的list中移除已经聚合的list
		for (AlarmAggrBean needToAggrAlarmAggrBean : needToAggrAlarmAggrBeanList) {
			alarmAggrBeanList.remove(needToAggrAlarmAggrBean);
		}
		// 本次出现过的报警，都需要加入到报警聚合中去。
		for (GpsInfo.AlarmItem alarmItem : alarmItemsList) {
			long alarmType = alarmItem.getAlarmType();
			// 判断之前是否包含这个报警
			boolean isContain = false;
			AlarmAggrBean preAlarmAggrBean = null;
			for (AlarmAggrBean alarmAggrBean : alarmAggrBeanList) {
				if (alarmAggrBean.getAlarmTypeId() == alarmItem.getAlarmType()) {
					isContain = true;
					preAlarmAggrBean = alarmAggrBean;
					break;
				}
			}
			if (isContain) {
				// 之前就有这种报警，更新之前报警的信息(更新count,endtime,avg等)
				fillAlarmAggrBean(preAlarmAggrBean, currentGps);
			} else {
				// 之前没有这种报警,为新的报警，需要添加到list中
				AlarmAggrBean alarmAggrBean = new AlarmAggrBean(currentGps.getVehicleID(), alarmType);
				fillAlarmAggrBean(alarmAggrBean, currentGps);
				addToVehicleId2AlarmAggrBeanList(vehicleId, alarmAggrBean);
			}
		}
		return needToAggrAlarmAggrBeanList;
	}

	/**
	 * @Title dealAreaInout
	 * @Description 处理进出区域报警
	 * @param vehicleId
	 * @param currentGps
	 * @param lastGps
	 *            void
	 * @author 韩欣宇
	 * @return
	 * @date 2016年2月24日 下午4:35:36
	 */
	private ArrayList<AreaInOutAlarmAggrBean> dealAreaInout(long vehicleId, GpsInfo.Gps currentGps, GpsInfo.Gps lastGps) {
		ArrayList<AreaInOutAlarmAggrBean> needToAggrAlarmAggrBeanList = new ArrayList<>();
		List<GpsInfo.AreaInout> areaInoutList = currentGps.getAreaInoutList();
		if (!vehicleId2AreaInOutAlarmAggrBeanList.containsKey(vehicleId)) {
			vehicleId2AreaInOutAlarmAggrBeanList.put(vehicleId, new ArrayList<AreaInOutAlarmAggrBean>());
		}
		ArrayList<AreaInOutAlarmAggrBean> areaInOutAlarmAggrBeanList = vehicleId2AreaInOutAlarmAggrBeanList.get(vehicleId);
		// 本次没有出现过的报警，但是之前出现过的报警，都需要被聚合结束掉
		for (AreaInOutAlarmAggrBean alarmAggrBean : areaInOutAlarmAggrBeanList) {
			boolean appearAgain = false;
			for (GpsInfo.AreaInout alarmItem : areaInoutList) {
				if (alarmAggrBean.getAlarmTypeId() == alarmItem.getAlarmTypeID() && alarmAggrBean.getAreaId() == alarmItem.getAreaID()) {
					appearAgain = true;
					break;
				}
			}
			if (!appearAgain) {
				needToAggrAlarmAggrBeanList.add(alarmAggrBean);
			}
		}
		for (AreaInOutAlarmAggrBean needToAggrAlarmAggrBean : needToAggrAlarmAggrBeanList) {
			areaInOutAlarmAggrBeanList.remove(needToAggrAlarmAggrBean);
		}
		// 本次出现过的报警，都需要加入到报警聚合中去。
		for (GpsInfo.AreaInout areaInout : areaInoutList) {
			long alarmType = areaInout.getAlarmTypeID();
			// 判断之前是否包含这个报警
			boolean isContain = false;
			AreaInOutAlarmAggrBean preAlarmAggrBean = null;
			for (AreaInOutAlarmAggrBean alarmAggrBean : areaInOutAlarmAggrBeanList) {
				if (alarmAggrBean.getAlarmTypeId() == areaInout.getAlarmTypeID() && alarmAggrBean.getAreaId() == areaInout.getAreaID()) {
					isContain = true;
					preAlarmAggrBean = alarmAggrBean;
					break;
				}
			}
			if (isContain) {
				// 之前就有这种报警
				fillAreaInOutAlarmAggrBean(preAlarmAggrBean, currentGps, areaInout);
			} else {
				// 之前没有这种报警
				AreaInOutAlarmAggrBean areaInOutAlarmAggrBean = new AreaInOutAlarmAggrBean(currentGps.getVehicleID(), alarmType);
				fillAreaInOutAlarmAggrBean(areaInOutAlarmAggrBean, currentGps, areaInout);
				addToVehicleId2AreaInOutAlarmAggrBeanList(vehicleId, areaInOutAlarmAggrBean);
			}
		}
		return needToAggrAlarmAggrBeanList;
	}

	/**
	 * @Title dealOverSpeed
	 * @Description 处理超速报警
	 * @param vehicleId
	 * @param currentGps
	 * @param lastGps
	 *            void
	 * @author 韩欣宇
	 * @return
	 * @date 2016年2月24日 下午4:37:40
	 */
	private ArrayList<OverSpeedAlarmAggrBean> dealOverSpeed(long vehicleId, GpsInfo.Gps currentGps, GpsInfo.Gps lastGps) {
		ArrayList<OverSpeedAlarmAggrBean> needToAggrAlarmAggrBeanList = new ArrayList<>();
		List<GpsInfo.OverSpeed> overSpeedList = currentGps.getOverSpeedList();
		if (!vehicleId2OverSpeedAlarmAggrBeanList.containsKey(vehicleId)) {
			vehicleId2OverSpeedAlarmAggrBeanList.put(vehicleId, new ArrayList<OverSpeedAlarmAggrBean>());
		}
		ArrayList<OverSpeedAlarmAggrBean> overSpeedAlarmAggrBeanList = vehicleId2OverSpeedAlarmAggrBeanList.get(vehicleId);
		// 本次没有出现过的报警，但是之前出现过的报警，都需要被聚合结束掉
		for (OverSpeedAlarmAggrBean alarmAggrBean : overSpeedAlarmAggrBeanList) {
			boolean appearAgain = false;
			for (GpsInfo.OverSpeed alarmItem : overSpeedList) {
				if (alarmAggrBean.getAlarmTypeId() == alarmItem.getAlarmTypeID() && alarmAggrBean.getAreaId() == alarmItem.getAreaID()) {
					appearAgain = true;
					break;
				}
			}
			if (!appearAgain) {
				needToAggrAlarmAggrBeanList.add(alarmAggrBean);
			}
		}
		
		for (OverSpeedAlarmAggrBean overSpeedAlarmAggrBean : needToAggrAlarmAggrBeanList) {
			overSpeedAlarmAggrBeanList.remove(overSpeedAlarmAggrBean);
		}
		
		// 本次出现过的报警，都需要加入到报警聚合中去。
		for (GpsInfo.OverSpeed overSpeed : overSpeedList) {
			long alarmType = overSpeed.getAlarmTypeID();
			// 判断之前是否包含这个报警
			boolean isContain = false;
			OverSpeedAlarmAggrBean preAlarmAggrBean = null;
			for (OverSpeedAlarmAggrBean alarmAggrBean : overSpeedAlarmAggrBeanList) {
				if (alarmAggrBean.getAlarmTypeId() == overSpeed.getAlarmTypeID() && alarmAggrBean.getAreaId() == overSpeed.getAreaID()) {
					isContain = true;
					preAlarmAggrBean = alarmAggrBean;
					break;
				}
			}
			if (isContain) {
				// 之前就有这种报警
				fillOverSpeedAlarmAggrBean(preAlarmAggrBean, currentGps, overSpeed);
			} else {
				// 之前没有这种报警
				OverSpeedAlarmAggrBean overSpeedAlarmAggrBean = new OverSpeedAlarmAggrBean(currentGps.getVehicleID(), alarmType);
				fillOverSpeedAlarmAggrBean(overSpeedAlarmAggrBean, currentGps, overSpeed);
				addToVehicleId2OverSpeedAlarmAggrBeanList(vehicleId, overSpeedAlarmAggrBean);
			}
		}
		return needToAggrAlarmAggrBeanList;
	}

	/**
	 * @Title dealOverTime
	 * @Description 处理超时/不足报警
	 * @param vehicleId
	 * @param currentGps
	 * @param lastGps
	 *            void
	 * @author 韩欣宇
	 * @return
	 * @date 2016年2月24日 下午4:37:42
	 */
	private ArrayList<OverTimeAlarmAggrBean> dealOverTime(long vehicleId, GpsInfo.Gps currentGps, GpsInfo.Gps lastGps) {
		ArrayList<OverTimeAlarmAggrBean> needToAggrAlarmAggrBeanList = new ArrayList<>();
		List<GpsInfo.OverTime> overTimeList = currentGps.getOverTimeList();
		if (!vehicleId2OverTimeAlarmAggrBeanList.containsKey(vehicleId)) {
			vehicleId2OverTimeAlarmAggrBeanList.put(vehicleId, new ArrayList<OverTimeAlarmAggrBean>());
		}
		ArrayList<OverTimeAlarmAggrBean> overTimeAlarmAggrBeanList = vehicleId2OverTimeAlarmAggrBeanList.get(vehicleId);
		// 本次没有出现过的报警，但是之前出现过的报警，都需要被聚合结束掉
		for (OverTimeAlarmAggrBean alarmAggrBean : overTimeAlarmAggrBeanList) {
			boolean appearAgain = false;
			for (GpsInfo.OverTime alarmItem : overTimeList) {
				if (alarmAggrBean.getAlarmTypeId() == alarmItem.getAlarmTypeID() && alarmAggrBean.getRoadId() == alarmItem.getRoadID()) {
					appearAgain = true;
					break;
				}
			}
			if (!appearAgain) {
				needToAggrAlarmAggrBeanList.add(alarmAggrBean);
			}
		}
		for (OverTimeAlarmAggrBean overTimeAlarmAggrBean : needToAggrAlarmAggrBeanList) {
			overTimeAlarmAggrBeanList.remove(overTimeAlarmAggrBean);
		}
		// 本次出现过的报警，都需要加入到报警聚合中去。
		for (GpsInfo.OverTime overTime : overTimeList) {
			long alarmType = overTime.getAlarmTypeID();
			// 判断之前是否包含这个报警
			boolean isContain = false;
			OverTimeAlarmAggrBean preAlarmAggrBean = null;
			for (OverTimeAlarmAggrBean alarmAggrBean : overTimeAlarmAggrBeanList) {
				if (alarmAggrBean.getAlarmTypeId() == overTime.getAlarmTypeID() && alarmAggrBean.getRoadId() == overTime.getRoadID()) {
					isContain = true;
					preAlarmAggrBean = alarmAggrBean;
					break;
				}
			}
			if (isContain) {
				// 之前就有这种报警
				fillOverTimeAlarmAggrBean(preAlarmAggrBean, currentGps, overTime);
			} else {
				// 之前没有这种报警
				OverTimeAlarmAggrBean areaInOutAlarmAggrBean = new OverTimeAlarmAggrBean(currentGps.getVehicleID(), alarmType);
				fillOverTimeAlarmAggrBean(areaInOutAlarmAggrBean, currentGps, overTime);
				addToVehicleId2OverTimeAlarmAggrBeanList(vehicleId, areaInOutAlarmAggrBean);
			}
		}
		return needToAggrAlarmAggrBeanList;
	}

	/**
	 * @Title fillAlarmAggrBean
	 * @Description 将gps信息填充到这个报警聚合中
	 * @param alarmAggrBean
	 * @param currentGps
	 * @author 韩欣宇
	 * @date 2016年2月24日 下午12:57:51
	 */
	private void fillAlarmAggrBean(AlarmAggrBean alarmAggrBean, GpsInfo.Gps currentGps) {
		/**
		 * 第一次出现的报警以及第一条报警信息
		 */
		if (alarmAggrBean.getCount() == 0) {
			alarmAggrBean.setStartTime(currentGps.getGpsDate());
			alarmAggrBean.setEndTime(currentGps.getGpsDate());
			alarmAggrBean.setDuration(1);
			alarmAggrBean.setCount(1);
			alarmAggrBean.setStartLocationId(currentGps.getLocationID());
			alarmAggrBean.setEndLocationId(currentGps.getLocationID());
			alarmAggrBean.setEnterpriseCode(currentGps.getEnterpriseCode());
			alarmAggrBean.setAvgSpeed((float) (currentGps.getSpeed() * 1.0 / HistoryExtend.SPEED_ZOOM));
			alarmAggrBean.setAvgDSpeed((float) (currentGps.getDrSpeed() * 1.0 / HistoryExtend.SPEED_ZOOM));
		} else {
			alarmAggrBean.setEndTime(currentGps.getGpsDate());
			alarmAggrBean.setDuration((int) ((alarmAggrBean.getEndTime() - alarmAggrBean.getStartTime()) / 1000) + 1);
			alarmAggrBean.setEndLocationId(currentGps.getLocationID());
			alarmAggrBean.setAvgSpeed((float) (avg(alarmAggrBean.getAvgSpeed(), alarmAggrBean.getCount(), (float) (currentGps.getSpeed() * 1.0 / HistoryExtend.SPEED_ZOOM))));
			alarmAggrBean.setAvgDSpeed((float) (avg(alarmAggrBean.getAvgDSpeed(), alarmAggrBean.getCount(), (float) (currentGps.getDrSpeed() * 1.0 / HistoryExtend.SPEED_ZOOM))));
			alarmAggrBean.setCount(alarmAggrBean.getCount() + 1);
		}
	}

	/**
	 * @Title avg
	 * @Description 重新计算平均值
	 * @param oldAvg
	 *            原来的平均值
	 * @param count
	 *            原来有多少个数去求的平均
	 * @param f
	 *            新加入的数字
	 * @return float
	 * @author 韩欣宇
	 * @date 2016年2月24日 下午12:11:36
	 */
	private float avg(float oldAvg, int count, float f) {
		return (oldAvg * count + f) / (count + 1);
	}

	/**
	 * @Title fillAreaInOutAlarmAggrBean
	 * @Description 将gps信息填充到这个报警聚合中
	 * @param areaInOutAlarmAggrBean
	 * @param currentGps
	 * @author 韩欣宇
	 * @date 2016年2月24日 下午12:57:51
	 */
	private void fillAreaInOutAlarmAggrBean(AreaInOutAlarmAggrBean areaInOutAlarmAggrBean, GpsInfo.Gps currentGps, GpsInfo.AreaInout areaInout) {
		fillAlarmAggrBean(areaInOutAlarmAggrBean, currentGps);
		areaInOutAlarmAggrBean.setAppendId(areaInout.getAppendID());
		areaInOutAlarmAggrBean.setAreaId(areaInout.getAreaID());
		areaInOutAlarmAggrBean.setLocationType(areaInout.getLocationType());
		areaInOutAlarmAggrBean.setDirection(areaInout.getDirection());
	}

	/**
	 * @Title fillOverSpeedAlarmAggrBean
	 * @Description 将gps信息填充到这个报警聚合中
	 * @param overSpeedAlarmAggrBean
	 * @param currentGps
	 * @param overSpeed
	 *            void
	 * @author 韩欣宇
	 * @date 2016年2月24日 下午1:15:23
	 */
	private void fillOverSpeedAlarmAggrBean(OverSpeedAlarmAggrBean overSpeedAlarmAggrBean, GpsInfo.Gps currentGps, GpsInfo.OverSpeed overSpeed) {
		fillAlarmAggrBean(overSpeedAlarmAggrBean, currentGps);
		overSpeedAlarmAggrBean.setAppendId(overSpeed.getAppendID());
		overSpeedAlarmAggrBean.setAreaId(overSpeed.getAreaID());
		overSpeedAlarmAggrBean.setLocationType(overSpeed.getLocationType());
	}

	/**
	 * @Title fillOverTimeAlarmAggrBean
	 * @Description 将gps信息填充到这个报警聚合中
	 * @param overTimeAlarmAggrBean
	 * @param currentGps
	 * @param overTime
	 *            void
	 * @author 韩欣宇
	 * @date 2016年2月24日 下午1:15:26
	 */
	private void fillOverTimeAlarmAggrBean(OverTimeAlarmAggrBean overTimeAlarmAggrBean, GpsInfo.Gps currentGps, GpsInfo.OverTime overTime) {
		fillAlarmAggrBean(overTimeAlarmAggrBean, currentGps);
		overTimeAlarmAggrBean.setAppendId(overTime.getAppendID());
		overTimeAlarmAggrBean.setRoadId(overTime.getRoadID());
		overTimeAlarmAggrBean.setDriveTime(overTime.getDriveTime());
		overTimeAlarmAggrBean.setResult(overTime.getResult());
	}

	/**
	 * @Title putAlarmAggrBeanToVehicleId2AlarmType2AlarmAggrBean
	 * @Description 将一个聚合的报警加入到map中
	 * @param vehicleId
	 * @param alarmAggrBean
	 *            void
	 * @author 韩欣宇
	 * @date 2016年2月24日 上午11:59:52
	 */
	private void addToVehicleId2AlarmAggrBeanList(long vehicleId, AlarmAggrBean alarmAggrBean) {
		ArrayList<AlarmAggrBean> alarmAggrBeanList = vehicleId2AlarmAggrBeanList.get(vehicleId);
		if (alarmAggrBeanList == null) {
			ArrayList<AlarmAggrBean> list = new ArrayList<AlarmAggrBean>();
			list.add(alarmAggrBean);
			vehicleId2AlarmAggrBeanList.put(vehicleId, list);
		} else {
			alarmAggrBeanList.add(alarmAggrBean);
		}
	}

	/**
	 * @Title addToVehicleId2AreaInOutAlarmAggrBeanList
	 * @Description 将一个聚合的报警加入到map中
	 * @param vehicleId
	 * @param areaInOutAlarmAggrBean
	 *            void
	 * @author 韩欣宇
	 * @date 2016年2月24日 下午2:22:37
	 */
	private void addToVehicleId2AreaInOutAlarmAggrBeanList(long vehicleId, AreaInOutAlarmAggrBean areaInOutAlarmAggrBean) {
		ArrayList<AreaInOutAlarmAggrBean> alarmAggrBeanList = vehicleId2AreaInOutAlarmAggrBeanList.get(vehicleId);
		if (alarmAggrBeanList == null) {
			ArrayList<AreaInOutAlarmAggrBean> list = new ArrayList<AreaInOutAlarmAggrBean>();
			list.add(areaInOutAlarmAggrBean);
			vehicleId2AreaInOutAlarmAggrBeanList.put(vehicleId, list);
		} else {
			alarmAggrBeanList.add(areaInOutAlarmAggrBean);
		}
	}

	/**
	 * @Title addToVehicleId2AreaInOutAlarmAggrBeanList
	 * @Description 将一个聚合的报警加入到map中
	 * @param vehicleId
	 * @param areaInOutAlarmAggrBean
	 *            void
	 * @author 韩欣宇
	 * @date 2016年2月24日 下午2:22:37
	 */
	private void addToVehicleId2OverSpeedAlarmAggrBeanList(long vehicleId, OverSpeedAlarmAggrBean overSpeedAlarmAggrBean) {
		ArrayList<OverSpeedAlarmAggrBean> alarmAggrBeanList = vehicleId2OverSpeedAlarmAggrBeanList.get(vehicleId);
		if (alarmAggrBeanList == null) {
			ArrayList<OverSpeedAlarmAggrBean> list = new ArrayList<OverSpeedAlarmAggrBean>();
			list.add(overSpeedAlarmAggrBean);
			vehicleId2OverSpeedAlarmAggrBeanList.put(vehicleId, list);
		} else {
			alarmAggrBeanList.add(overSpeedAlarmAggrBean);
		}
	}

	/**
	 * @Title addToVehicleId2AreaInOutAlarmAggrBeanList
	 * @Description 将一个聚合的报警加入到map中
	 * @param vehicleId
	 * @param areaInOutAlarmAggrBean
	 *            void
	 * @author 韩欣宇
	 * @date 2016年2月24日 下午2:22:37
	 */
	private void addToVehicleId2OverTimeAlarmAggrBeanList(long vehicleId, OverTimeAlarmAggrBean overTimeAlarmAggrBean) {
		ArrayList<OverTimeAlarmAggrBean> alarmAggrBeanList = vehicleId2OverTimeAlarmAggrBeanList.get(vehicleId);
		if (alarmAggrBeanList == null) {
			ArrayList<OverTimeAlarmAggrBean> list = new ArrayList<OverTimeAlarmAggrBean>();
			list.add(overTimeAlarmAggrBean);
			vehicleId2OverTimeAlarmAggrBeanList.put(vehicleId, list);
		} else {
			alarmAggrBeanList.add(overTimeAlarmAggrBean);
		}
	}

}