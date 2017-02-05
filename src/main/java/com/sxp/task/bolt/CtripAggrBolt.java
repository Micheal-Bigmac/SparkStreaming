package com.sxp.task.bolt;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.hbase.geography.CtripEntity;
import com.hsae.hbase.geography.GeographyUtil;
import com.hsae.hbase.table.History;
import com.hsae.hbase.util.DateUtil;
import com.sxp.task.protobuf.generated.CtripAggrInfo;
import com.sxp.task.protobuf.generated.GpsInfo;
import com.sxp.task.util.GpsComparator;
import org.apache.storm.tuple.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @param <bulider1>
 * @ClassName: CtripAggrBolt.java
 * @Description:行程聚合bolt
 * @author: 熊尧
 * @company: 上海势航网络技术有限公司
 * @date 2016年3月15日
 */
public class CtripAggrBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(CtripAggrBolt.class);
    private static final long serialVersionUID = 1L;

    public static final String GROUPING_FIELD = "vehicleid";
    public static final String CTRIPAGGR_FIELD = "ctripAggr";
    private Map<Long, GpsInfo.Gps> vehicleId2LastGps = new HashMap<Long, GpsInfo.Gps>();
    private Map<Long, List<GpsInfo.Gps>> vehicleIdGpsList = new HashMap<Long, List<GpsInfo.Gps>>();

    public void prepare(Map stormConf) {
//        this.collector = collector;
    }

    @SuppressWarnings("unchecked")
    public void execute(Tuple tuple) {
        Long vehicleId = (Long) tuple.getValue(0);//tuple中的第一个参数车辆id
        List<byte[]> bytesList = (List<byte[]>) tuple.getValue(1);//车辆信息列表

        List<CtripEntity> ctripSegmentList = new ArrayList<>();
        List<CtripEntity> gpsSegmentList = new ArrayList<>();

        try {
            //根据车辆id对车辆进行排序
            TreeSet<GpsInfo.Gps> gpsSet = new TreeSet<>(GpsComparator.GPS_COMPARATOR);
            for (byte[] bytes : bytesList) {
                GpsInfo.Gps gps = GpsInfo.Gps.parseFrom(bytes);
                gpsSet.add(gps);
            }
            for (GpsInfo.Gps currentGps : gpsSet) {
                long vid = currentGps.getVehicleID();
                if (vehicleId2LastGps.containsKey(vid)) {
                }
                GpsInfo.Gps oldGps = vehicleId2LastGps.get(vid);
                if (oldGps == null) {
                    vehicleId2LastGps.put(vid, currentGps);
                }
            }
        } catch (InvalidProtocolBufferException e) {
            LOG.error("protobuf parse gps error! ", e);
        }

        //计算车辆行程序号，开始时间，结束时间，开始rowkey,结束rowkey，是否持续到下一天
        ctripSegmentList = calculateCtripSegment(bytesList);
        List<byte[]> ctripListBytes = new ArrayList<>();
        try {
            //计算车辆行驶里程，车辆记录仪行驶里程
            gpsSegmentList = ctripInfo(bytesList, ctripSegmentList);
            //将对象序列化为二进制数组格式，并添加到ctripListBytes列表
            if (gpsSegmentList != null) {

                for (CtripEntity gpsCtrip : gpsSegmentList) {
                    CtripAggrInfo.CtripAggr.Builder bulider = CtripAggrInfo.CtripAggr.newBuilder();
                    bulider.setOrderId(gpsCtrip.getOrderId());
                    bulider.setSrowkey(ByteString.copyFrom(gpsCtrip.getStartRowkey()));
                    bulider.setErowkey(ByteString.copyFrom(gpsCtrip.getEndRowkey()));
                    bulider.setStime(ByteString.copyFrom(gpsCtrip.getStartBCDTime()));
                    bulider.setEtime(ByteString.copyFrom(gpsCtrip.getEndBCDTime()));
                    bulider.setGpsmile(gpsCtrip.getGpsMileage());
                    bulider.setGpsavgspd(gpsCtrip.getGpsSpeed());
                    bulider.setRmile(gpsCtrip.getRMileage());
                    bulider.setRavgspd(gpsCtrip.getRSpeed());
                    bulider.setIsnextday(gpsCtrip.isNextDay());
                    CtripAggrInfo.CtripAggr vo = bulider.build();
                    byte[] v = vo.toByteArray();
                    ctripListBytes.add(v);
                }
            }
        } catch (InvalidProtocolBufferException e) {
            LOG.error("protobuf parse gps error! ", e);
        }
//        collector.emit(tuple, new Values(vehicleId, ctripListBytes));
    }

    //计算行程聚合相关信息
    private List<CtripEntity> calculateCtripSegment(List<byte[]> ctripList) {

        List<CtripEntity> ctripResult = new ArrayList<CtripEntity>();
        //根据ACC开关状态变化，获得形成区间
        boolean lastAccStatus = false;
        int i = 0, j = 0;
        int orderId = 0;            //行程序号
        byte[] startRowkey = null;  //开始行健
        byte[] endRowkey = null;    //结束行健
        byte[] startBCDTime = null; // 行程开始时间
        byte[] endBCDTime = null;   // 行程结束时间
        boolean isNextDay = false;  //行程是否持续到下一天

        for (int m = 0, size = ctripList.size(); m < size; m++) {
            byte[] bytes = ctripList.get(m);
            try {
                GpsInfo.Gps gps = GpsInfo.Gps.parseFrom(bytes);
                i++;

                if (!lastAccStatus && gps.getACC()) {
                    j++;
                    orderId = j;
                    startBCDTime = DateUtil.getBCDDateTime(new Date(gps.getGpsDate()));
                    startRowkey = History.buildRowKeyUsingReverseVehicleIdAndBCDDateTime(gps.getVehicleID(), gps.getGpsDate());
                    lastAccStatus = true;
                } else if (lastAccStatus && !gps.getACC()) {
                    endBCDTime = DateUtil.getBCDDateTime(new Date(gps.getGpsDate()));
                    endRowkey = History.buildRowKeyUsingReverseVehicleIdAndBCDDateTime(gps.getVehicleID(), gps.getGpsDate());
                    lastAccStatus = false;
                    // 设置行程实体
                    CtripEntity ctripEntity = new CtripEntity();
                    ctripEntity.setOrderId(orderId);
                    ctripEntity.setStartRowkey(startRowkey);
                    ctripEntity.setEndRowkey(endRowkey);
                    ctripEntity.setStartBCDTime(startBCDTime);
                    ctripEntity.setEndBCDTime(endBCDTime);

                    ctripResult.add(ctripEntity);
                }

                if (i == ctripList.size() && gps.getACC()) {// 最后一条ACC是开的
                    endBCDTime = DateUtil.getBCDDateTime(new Date(gps.getGpsDate()));
                    endRowkey = History.buildRowKeyUsingReverseVehicleIdAndBCDDateTime(gps.getVehicleID(), gps.getGpsDate());
                    lastAccStatus = false;
                    isNextDay = true;// 标记为持续到下一天

                    // 设置行程实体
                    CtripEntity ctripEntity = new CtripEntity();
                    ctripEntity.setOrderId(orderId);
                    ctripEntity.setStartRowkey(startRowkey);
                    ctripEntity.setEndRowkey(endRowkey);
                    ctripEntity.setStartBCDTime(startBCDTime);
                    ctripEntity.setEndBCDTime(endBCDTime);
                    ctripEntity.setNextDay(isNextDay);// 标记为持续到下一天

                    ctripResult.add(ctripEntity);
                }
            } catch (InvalidProtocolBufferException e) {
                LOG.error("protobuf parse gps error! ", e);
            }
        }
        if (ctripResult.size() > 0) {// 如有行程数据
            return ctripResult;
        } else
            return null;
    }

    /**
     * 计算行程信息（里程、平均速度）
     *
     * @param optimized
     * @param calationResult
     * @return
     * @throws InvalidProtocolBufferException
     */

    public static List<CtripEntity> ctripInfo(List<byte[]> ctripList,
                                              List<CtripEntity> calationResult) throws InvalidProtocolBufferException {

        if (calationResult != null && calationResult.size() > 0) {// 有行程分段信息
            List<CtripEntity> calationResultRet = new ArrayList<CtripEntity>(calationResult);// 复制一份

            for (int k = 0; k < calationResult.size(); k++) {// 按照行程分段循环

                CtripEntity ctripEntity = calationResult.get(k);// 获得当前行程的实体；

                byte[] f_startBCDTime = ctripEntity.getStartBCDTime();// 获得行键中是时分秒最为结束时间
                byte[] f_endBCDTime = ctripEntity.getEndBCDTime();// 行程结束时间

                Date f_startDateTime = DateUtil.bCDtoDate(f_startBCDTime);
                Date f_endDateTime = DateUtil.bCDtoDate(f_endBCDTime);


                double GpsMileage = 0;// GPS里程
                float RMileageSum = 0f, lastRMileage = 0f, currentRMileage = 0f;// 行驶记录仪里程
                float GpsSpeedSum = 0f;// 行驶时GPS速度和

                float RSpeedSum = 0;// 行驶时行驶记录仪速度

                double lat1 = 0, lng1 = 0, lat2 = 0, lng2 = 0;

                // 计算GPS里程
                if (ctripList != null && ctripList.size() > 0) {// 有优化后的点

                    int optimizedCount = 0;// 优化后的点每个区间内的记录数

                    for (int m = 0; m < ctripList.size(); m++) {

                        byte[] bytes = ctripList.get(m);

                        GpsInfo.Gps gpsInfo = GpsInfo.Gps.parseFrom(bytes);

                        long optimizedTime = DateUtil.bCDtoDate(DateUtil.getBCDDateTime(new Date(gpsInfo.getGpsDate()))).getTime();

                        if (optimizedTime >= f_startDateTime.getTime() && optimizedTime <= f_endDateTime.getTime()) {// 优化后的点位于当前区间时间段内
                            optimizedCount++;// 优化后的点每个区间内的记录数增加1;

                            // 计算GPS里程
                            if (optimizedCount == 1) {
                                lat1 = gpsInfo.getLatitude();
                                lng1 = gpsInfo.getLongitude();

                                // 行驶记录仪里程
                                lastRMileage = gpsInfo.getMileage();

                            } else {
                                lat2 = gpsInfo.getLatitude();
                                lng2 = gpsInfo.getLongitude();

                                // 计算GPS里程
                                GpsMileage += GeographyUtil.getDistance(lat1, lng1, lat2, lng2);

                                lat1 = lat2;
                                lng1 = lng2;

                                // 计算行驶记录仪里程
                                currentRMileage = gpsInfo.getMileage();
                                RMileageSum += currentRMileage - lastRMileage;

                                lastRMileage = currentRMileage;
                            }

                            // 计算GPS平均速度
                            GpsSpeedSum += gpsInfo.getSpeed();

                            // 计算记录仪的平均速度
                            RSpeedSum += gpsInfo.getDrSpeed();

                        }
                    }
                    if (optimizedCount > 1) {
                        ctripEntity.setGpsMileage(GpsMileage);// GPS里程
                        ctripEntity.setRMileage(RMileageSum);// 行驶记录仪里程
                        ctripEntity.setGpsSpeed(GpsSpeedSum / optimizedCount);// GPS平均速度
                        ctripEntity.setRMileage(RSpeedSum / optimizedCount);// 行驶记录仪平均速度

                        calationResultRet.set(k, ctripEntity);// 更新行程信息
                    }
                }
            }
            return calationResultRet;
        } else
            return null;
    }
}
