//package com.dorr.pingan.spark.tags;
//
//import java.util.BitSet;
//
//public class Data {
//    String id;
//    double fx;
//    double zx = 1;
//    BitSet tags;
//    String[] tag_arr;
//
//    publi
//        this.id = id;
//        this.fx = fx;
//        this.zx = zx;
//        this.tags = new BitSet();
//    }
//
//
//    public void setId(String id) {
//        this.id = id;
//    }
//
//    public void setFx(double fx) {
//        this.fx = fx;
//    }
//
//    public void setZx(double zx) {
//        this.zx = zx;
//    }
//
//    public String getId() {
//        return id;
//    }
//
//    public double getFx() {
//        return fx;
//    }
//
//    public double getZx() {
//        return zx;
//    }
//
//    public Data setTags(String tags) {
//        String[] split = tags.split(",");
//        tag_arr = split;
//        for (String s : split) {
//            this.tags.set(Integer.parseInt(s.trim()));
//        }
//        return this;
//    }
//
//    @Override
//    public String toString() {
//        return "Data{" +
//                "id='" + id + '\'' +
//                ", fx=" + fx +
//                ", zx=" + zx +
//                '}';
//    }
//
//    public BitSet getTags() {
//        return tags;
//    }
//
//}
