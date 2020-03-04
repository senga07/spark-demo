package com.hlsijx.spark.sql.project.stat_traffic.model

/**
  * 定义数据库中对象字段
  * create table video_access_traffic_stat(
  * cmsid int(11) ,
  * date varchar(20),
  * traffic_sum bigint(20),
  * primary key(cmsid,date)
  * );
  */
case class VideoAccessTrafficStat(cmsId : Long, date : String, trafficSum : Long)
