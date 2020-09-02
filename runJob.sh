#!/usr/bin/env bash


function getDates {
  local _dates=""
  for f in $(hdfs dfs -stat '%n' /apps/hive/warehouse/erdata/*|sort -nr|grep [0-9\][0-9\][0-9\][0-9\]-[0-9\][0-9\]-[0-9\][0-9\]) ; do
    local _d=$(echo $f)
    dates+="$_d"
  done
  echo $_dates
}

jar=er-spark.jar


spark-submit --jar