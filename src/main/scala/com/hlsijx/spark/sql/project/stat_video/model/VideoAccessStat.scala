package com.hlsijx.spark.sql.project.stat_video.model

/**
  * 定义数据库中对象字段
  * create table video_access_stat(
  * cmsid int(11) ,
  * date varchar(20),
  * total int(11),
  * primary key(cmsid,date)
  * );
  */
case class VideoAccessStat(cmsId : Long, date : String, total : Long)
