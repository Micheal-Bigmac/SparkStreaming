﻿# SparkStreaming
用来测试SparkStreaming 和Storm 性能




/**
  * @author  BigMac Micheal
  * Created by PC on 2017/1/26.
  *
  * 该程序 故意留有两到三处
  *     1  配置文件在 driver 加载后通过 property 重新加载外部自定义的配置  可以做到 加载一次
  *     2  写入数据库(改Db2) 发生 GC 会频繁  通过SparkStreaming  调优 发现 延迟率高 下行写入速度 小于上行下发速度  (优化代码中已经有 并未在改程序中改写)  程序中做了 控制
  *     3  driver  中 Hbase 数据库相关操作 可以在一个foreachRDD  中完成业务处理（性能上可能会有影响 官网给出建议 和 在github 中看到的）
  *     4 有些线程中需要的缓存数据 个人只是按照个人方式实现 无法在程序暂停杀掉后 加载程序业务逻辑中的缓存 （缓存和Spark 中的缓存不一样 业务中的缓存 类似缓存上次结束或者  开始的数据 与当前每条数据比较 ）
  *     5  写入zookeeper  过时问题 已经修改
  * 上面提出的问题是发现留下的问题 代码中留有解决方案只是 没有在该主程序中更改  如果你发现其他问题 请留言 非常感谢
  *
  *
  *  该程序  一开始想写个SparkStreaming DirectKafka Zookeeper 的框架  外部加入一些配置文件  同时支持 Hbase 关系型数据库 的外部存储
  *  后来 写着写着 就把 公司Storm 项目改成sparkStreaming  同时在公司线下跑过测试
  *      改写思想：  几乎不更改原先的代码逻辑或者不动原先代码  自己对Strom bolt tuple 和 数据发送 重新抽象
  *                关于Storm 内部的netty 数据发送 变成手动去处理
  *                storm bolt 缓存部分 提到 driver 代码块区
  *                Java Scala 不同类型的转换 通过显式 或隐式调用 （耗时最长）
  *
  * 疑问   前提： scala  中 变量 在参数传递 故意设计不可变 在spark 中通过 transform  转换 返回的值变成RDD[Any]  在后面transform action 还要用到
  *        1 ： spark 中是在算子 转换过程中是对变量进行内存copy 生成新的变量（地址+内存空间）  还是使用原先的内存地址空间  涉及到GC 问题
  *
  *
  *  耗时 1-2周左右
  *
  */