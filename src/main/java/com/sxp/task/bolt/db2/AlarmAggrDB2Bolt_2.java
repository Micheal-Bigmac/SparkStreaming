/**
 * @ClassName
 * @Description
 * @author 李小辉
 * @company 上海势航网络科技有限公司
 * @date 2016年10月19日
 */
package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.hbase.util.DateUtil;
import com.hsae.rdbms.db2.DB2ExcutorQuenue;
import com.sxp.task.protobuf.generated.AlarmAggrInfo.AlarmAggr;
import org.apache.storm.tuple.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AlarmAggrDB2Bolt_2 extends BaseRichBolt {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(AlarmAggrDB2Bolt_2.class);
    static String TABLE_NAME = "T_ALARM_AGGR_";
    static String TABLE_ERROR_NAME = "T_ALARM_AGGR_ERR";
    private static Connection connection = null;

    public void prepare(Map stormConf) {
        connection = DB2ExcutorQuenue.isExstConnection(connection);
    }

    public void execute(Tuple input) {
        List<byte[]> dataList = (List<byte[]>) input.getValue(0);
        long start = 0;
        List<String> sql = null;
        if (dataList.size() == 0)
            return;
        try {
            sql = buildPreparedSqlAndValuesList(dataList);
            start = System.currentTimeMillis();
            if (sql == null || sql.size() == 0) {
                LOG.warn("no preparedSqlAndValuesList can be built from " + dataList.size() + " dataList.");
            } else {
                DB2ExcutorQuenue.excuteBatch(sql, connection);
            }
        } catch (InvalidProtocolBufferException e) {
            LOG.error(this.getClass().getName() + ".execute error!");
        } catch (IllegalArgumentException e) {
            LOG.error(this.getClass().getName() + ".execute error!");
        } catch (SQLException e) {
//            if (sql != null && sql.size() > 0)
//			_collector.emit("404", new Values(sql));
        } finally {
            LOG.warn("execute sql for " + dataList.size() + " records in " + ((System.currentTimeMillis() - start)) + "ms");
        }
    }

    protected List<String> buildPreparedSqlAndValuesList(List<byte[]> alarmAgrrList) throws InvalidProtocolBufferException {
        List<String> Sqls = new ArrayList<String>();
        StringBuffer alarmAggrSql = null;
        AlarmAggr alarmAggr = null;
        String gpsTime = null;
        String TABLE_NAME_DATE = null;
        for (int i = 0, size = alarmAgrrList.size(); i < size; i++) {

            alarmAggr = AlarmAggr.parseFrom(alarmAgrrList.get(i));
            if (DateUtil.subtractOneDay() <= alarmAggr.getStartTime() && DateUtil.addOneDay() >= alarmAggr.getStartTime()) {
                alarmAggrSql = new StringBuffer();
                gpsTime = DateUtil.getStrTime(alarmAggr.getStartTime(), "yyyyMMdd");
                TABLE_NAME_DATE = TABLE_NAME + gpsTime;
                alarmAggrSql.append("INSERT INTO ").append(TABLE_NAME_DATE).append("(F_ID,F_ALARM_ID,F_VEHICLE_ID,F_PLATE_CODE,F_START_TIME,F_END_TIME,F_COUNT,F_STATUS,F_USER_ID,F_CONTEXT,F_START_ID,F_END_ID,F_UPDATE_TIME,F_ENTERPRISE_CODE,F_SPEEDINGVALUE,F_ALARMBAK,F_RULESID,F_SPEED,F_DSPEED)VALUES(");
                SqlPackage(alarmAggrSql, alarmAggr, Sqls);
            } else {
                alarmAggrSql = new StringBuffer();
                alarmAggrSql.append("INSERT INTO ").append("T_ALARM_AGGR_ERR").append("(F_ID,F_ALARM_ID,F_VEHICLE_ID,F_PLATE_CODE,F_START_TIME,F_END_TIME,F_COUNT,F_STATUS,F_USER_ID,F_CONTEXT,F_START_ID,F_END_ID,F_UPDATE_TIME,F_ENTERPRISE_CODE,F_SPEEDINGVALUE,F_ALARMBAK,F_RULESID,F_SPEED,F_DSPEED)VALUES(");
                SqlPackage(alarmAggrSql, alarmAggr, Sqls);
            }
        }
        return Sqls;
    }

    private void SqlPackage(StringBuffer alarmAggrSql, AlarmAggr alarmAggr, List<String> Sqls) {
        alarmAggrSql.append(alarmAggr.getID()).append(",").append(alarmAggr.getAlarmTypeID()).append(",").append(alarmAggr.getVehicleID()).append(",'").append(alarmAggr.getPlateCode()).append("','");
        String startDate = DateUtil.getStrTime(alarmAggr.getStartTime(), "yyyy-MM-dd HH:mm:ss");
        String endDate = DateUtil.getStrTime(alarmAggr.getEndTime(), "yyyy-MM-dd HH:mm:ss");
        alarmAggrSql.append(startDate).append("','").append(endDate).append("',").append(alarmAggr.getCount()).append(",").append(alarmAggr.getStatus()).append(",");
        if (alarmAggr.hasUserID())
            alarmAggrSql.append(alarmAggr.getUserID());
        else
            alarmAggrSql.append("null");
        alarmAggrSql.append(",'");
        alarmAggrSql.append(alarmAggr.getContext()).append("',");
        alarmAggrSql.append(alarmAggr.getStartID()).append(",").append(alarmAggr.getEndID()).append(",'");
        String updateDate = DateUtil.getStrTime(alarmAggr.getUpdateTime(), "yyyy-MM-dd HH:mm:ss");
        alarmAggrSql.append(updateDate).append("','").append(alarmAggr.getEnterpriseCode()).append("',").append(alarmAggr.getSpeedingValue()).append(",'").append(alarmAggr.getAlarmRemark()).append("',").append(alarmAggr.getRulesID()).append(",").append(alarmAggr.getSpeed()).append(",");
        if (alarmAggr.hasDSpeed())
            alarmAggrSql.append(alarmAggr.getDSpeed());
        else
            alarmAggrSql.append("null");
        alarmAggrSql.append(")");
        Sqls.add(alarmAggrSql.toString());
    }


}
