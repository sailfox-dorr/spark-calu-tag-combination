package com.dorr.pingan.spark.tag.cal

import java.util

class UserTag {
  var id: String = _;
  var fx: Double = _;
  var zx: Double = _;
  var tagsBit = new util.BitSet()

  override def toString = s"UserTag($id, $fx, $zx)"

  def setTags(tags: String) = {
    val splits = tags.split(",")
    splits.foreach(s => tagsBit.set(s.trim.toInt))
    this
  }

  def this(id: String, fx: Double, zx: Double) = {
    this()
    this.id = id
    this.fx = fx
    this.zx = zx
  }
}