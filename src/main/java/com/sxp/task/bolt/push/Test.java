/**
 * @ClassName 
 * @Description 
 * @author 李小辉
 * @company 上海势航网络科技有限公司
 * @date 2016年10月24日
 */
package com.sxp.task.bolt.push;

import cn.jpush.api.common.resp.APIConnectionException;
import cn.jpush.api.common.resp.APIRequestException;

public class Test {
	public static void main(String[] args) throws APIConnectionException, APIRequestException {
//		JPushManager.getJPushManagerInstance().Push("18071adc03052f19d73", "大鹏兄，我给你推了条消息，收到没？", "123","沪A123456");
		JPushManager.getJPushManagerInstance().Push("1104a89792aff995ca6", "大鹏兄，我给你推了条消息，收到没？", "123","沪A123456");

	}
}
