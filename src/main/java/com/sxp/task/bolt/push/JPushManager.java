package com.sxp.task.bolt.push;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;

import cn.jpush.api.JPushClient;
import cn.jpush.api.common.resp.APIConnectionException;
import cn.jpush.api.common.resp.APIRequestException;
import cn.jpush.api.push.PushResult;
import cn.jpush.api.push.model.Platform;
import cn.jpush.api.push.model.PushPayload;
import cn.jpush.api.push.model.audience.Audience;
import cn.jpush.api.push.model.audience.AudienceTarget;
import cn.jpush.api.push.model.notification.AndroidNotification;
import cn.jpush.api.push.model.notification.IosNotification;
import cn.jpush.api.push.model.notification.Notification;

/**
 * @author xiongyao
 *
 */
public class JPushManager {
	private static final Logger LOG = LoggerFactory.getLogger(JPushManager.class);
	
	private static JPushManager jPushManager = new JPushManager();
	private static String app_key = "5d48f1a3b0209040e8673391";//原生态的
	private static String master_secret = "60c8a5ad2ae737ab55eb567a";//原生态的
//	private static String app_key = "6491bcda873c1c53b9e5b38b";
//	private static String master_secret = "b6e61fb40152cb2ec882c4d0";
	
	private static JPushClient jPushClient = new JPushClient(master_secret, app_key);

	private JPushManager() {
	}

	public static JPushManager getJPushManagerInstance() {
		if (jPushManager == null) {
			return jPushManager = new JPushManager();
		}
		return jPushManager;
	}
	
	public void Push(String registrationId, String content, String vehicleId, String plateCode) throws APIConnectionException, APIRequestException{
		 if (jPushClient == null)
		{
			try {
				throw new Exception("推送Client未初始化！");
			} catch (Exception e) {
			}
		}
		PushPayload payload = PushObject_android_and_ios(registrationId, content, vehicleId, plateCode);
		PushResult result;
		result = jPushClient.sendPush(payload);

	}
	private PushPayload PushObject_android_and_ios(String registrationId, String msg, String vehicleId, String plateCode)
    {
		PushPayload.Builder builder = PushPayload.newBuilder();
		
		Audience.Builder audienceBuilder = Audience.newBuilder();
		if(null != registrationId && registrationId != ""){
			audienceBuilder.addAudienceTarget(AudienceTarget.registrationId(registrationId));
		}
		
		Notification.Builder notificationBuilder = Notification.newBuilder();
		
		JsonObject json = new JsonObject();
		json.addProperty("VehicleId", vehicleId);
		json.addProperty("PlateCode", plateCode);
		
		IosNotification.Builder iosNotificationBuilder = IosNotification.newBuilder();
		iosNotificationBuilder.setAlert(msg);
		iosNotificationBuilder.addExtra("EXTRA_EXTRA", json);
		AndroidNotification.Builder androidNotificationBuilder = AndroidNotification.newBuilder();
		androidNotificationBuilder.setAlert(msg);
		androidNotificationBuilder.addExtra("EXTRA_EXTRA", json);
		
		notificationBuilder.addPlatformNotification(iosNotificationBuilder.build())
				.addPlatformNotification(androidNotificationBuilder.build());

		builder.setAudience(audienceBuilder.build());
		builder.setPlatform(Platform.all());
		builder.setNotification(notificationBuilder.build());
		return builder.build();
    }
}