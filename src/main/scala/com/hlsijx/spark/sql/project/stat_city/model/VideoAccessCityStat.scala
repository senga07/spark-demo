package com.hlsijx.spark.sql.project.stat_city.model

/**
  * 定义数据库中对象字段
  * create table video_access_city_stat(
  * cmsid int(11) ,
  * date varchar(20),
  * city varchar(50),
  * total int(11),
  * rank int(11),
  * primary key(cmsid,date,city)
  * );
  */
case class VideoAccessCityStat(cmsId : Long, date : String, total : Long, city : String, rank : Int)
