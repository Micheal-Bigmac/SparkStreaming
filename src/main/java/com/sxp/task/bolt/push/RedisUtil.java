package com.sxp.task.bolt.push;

import com.alibaba.fastjson.JSONObject;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;


public class RedisUtil implements Serializable{
	
	private static JedisCluster JEDIS_CLUSTER = null;
	
	public  JedisCluster getJedisClusterInstance(){
		if(JEDIS_CLUSTER == null){
			Set<HostAndPort> set = new HashSet<HostAndPort>();
			set.add(new HostAndPort("10.10.11.16", 7000));
			set.add(new HostAndPort("10.10.11.16", 7001));
			set.add(new HostAndPort("10.10.11.16", 7002));
			set.add(new HostAndPort("10.10.11.16", 7003));
			set.add(new HostAndPort("10.10.11.16", 7004));
			set.add(new HostAndPort("10.10.11.16", 7005));
			JEDIS_CLUSTER = new JedisCluster(set);
		}
		return JEDIS_CLUSTER;
	}
	
	public String get(String vehicleId){
		JedisCluster jedisCluster = getJedisClusterInstance();
		return jedisCluster.get(vehicleId);
	}
	
	public Vehicle getVehicle(String vehicleId){
		String json = get(vehicleId);
		Vehicle vehicle = (Vehicle) JSONObject.parseObject(json, Vehicle.class);
		return vehicle;
	}
}
