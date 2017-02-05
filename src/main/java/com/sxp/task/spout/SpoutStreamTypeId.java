package com.sxp.task.spout;

/**
 * Created by PC on 2017/1/10.
 */
public enum SpoutStreamTypeId {

    _100001("100001"), // Gps
    _100002("100002"), // Append
    _100003("100003"), // Can
    _100004("100004"), // Alarm
    _100005("100005"), // AlarmAggr
    _100006("100006"), // AlarmAggrRelation
    _100007("100007"), // AreaInout
    _100008("100008"), // ExceptionAlarm
    _100009("100009"), // OilMass
    _100010("100010"), // OverSpeed
    _100011("100011"), // OverTime
    _100012("100012"), // TriaxialData
    _100013("100013"), // UnreportedGps
    _100014("100014"), // YouWeiTemp
    _100015("100015"), // GBOperativeNorm
    _100016("100016"), // GBDrivingLicense
    _100017("100017"), // GBRealTime
    _100018("100018"), // GBTotalMileage
    _100019("100019"), // GBPulseModulus
    _100020("100020"), // GBVehicleInfo
    _100021("100021"), // GBSignalConfig
    _100022("100022"), // GBUniqueNumber
    _100023("100023"), // GBTravelSpeed
    _100024("100024"), // GBLocationInfo
    _100025("100025"), // GBDoubtfulAccident
    _100026("100026"), // GBOvertimeDriving
    _100027("100027"), // GBDriverRecord
    _100028("100028"), // GBExternalPower
    _100029("100029"), // GBParameterModify
    _100030("100030"), // GBSpeedState
    _100031("100031"), // StrokeAlarm
    _100032("100032"), // CardUpload
    _100033("100033"), // ProgramVersion
    _100034("100034"), // OnLine
    _100035("100035"), // DeviceReg
    _100036("100036"), // DeviceLogOut
    _100037("100037"), // AuthorityLog
    _100038("100038"), // DeviceParms
    _100039("100039"), // TrackQuery
    _100040("100040"), // VehicleControl
    _100041("100041"), // WayBill
    _100042("100042"), // DriverIdentity
    _100043("100043"), // Media
    _100044("100044"), // MediaEvent
    _100045("100045"), // MediaSearch
    _100046("100046"), // AreaInOut
    _100060("100060"); // test

    private String value;

    SpoutStreamTypeId(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

}
