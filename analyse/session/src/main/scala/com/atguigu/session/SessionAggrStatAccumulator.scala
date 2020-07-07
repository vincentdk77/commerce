/*
 * Copyright (c) 2018. Atguigu Inc. All Rights Reserved.
 */

package com.atguigu.session

import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable

/**
  * 自定义累加器
  */
class SessionAggrStatAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]] {

  // 保存所有聚合数据
  private val aggrStatMap = mutable.HashMap[String, Int]()

  override def isZero: Boolean = {
    aggrStatMap.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val newAcc = new SessionAggrStatAccumulator
    aggrStatMap.synchronized{
      //todo 拼接两个map
      newAcc.aggrStatMap ++= this.aggrStatMap
    }
    newAcc
  }

  override def reset(): Unit = {
    aggrStatMap.clear()
  }

  override def add(v: String): Unit = {
    if (!aggrStatMap.contains(v))
      aggrStatMap += (v -> 0)
    aggrStatMap.update(v, aggrStatMap(v) + 1)
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
      case acc: SessionAggrStatAccumulator => {
        //acc.aggrStatMap是初始值，遍历this.aggrStatMap的元素依次与前面的进行累加（不一定是累加，具体根据内部实现操作来）操作
        acc.aggrStatMap.foldLeft(this.aggrStatMap){
          //简化写法
//        (this.aggrStatMap /: acc.value) {
          case (map, (k, v)) => {
            map += (k -> (v + map.getOrElse(k, 0)))
          }
        }
      }

    }
  }

  override def value: mutable.HashMap[String, Int] = {
    this.aggrStatMap
  }
}
