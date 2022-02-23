package com.dorr.pingan.spark.tags;

import com.github.javafaker.Faker;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Locale;
import java.util.Random;
import java.util.Set;

public class MockData {
    private static Random random = new Random();
    private static String base = "abcdefghijklmnopqrstuvwxyz1234567890";


    public static void main(String[] args) throws FileNotFoundException {
        Faker faker = new Faker(Locale.ENGLISH);
//        PrintStream printStream = new PrintStream("E:\\github\\My\\DataStructure\\src\\main\\resources\\puhui.csv");
        PrintStream printStream = new PrintStream("E:\\github\\My\\DataStructure\\src\\main\\resources\\puhui_num.csv");
        System.setOut(printStream);
        for (int i = 0; i < 200000; i++) {
            String id = faker.name().nameWithMiddle().replace(" ", "") + new Random().nextInt(100);
            double fx = new Random().nextInt(2);
            Set<String> set = getZuheNum(2000);
            DataBeam dataBeam = new DataBeam(set, id, fx, 1);
            System.out.println(dataBeam);
        }
    }

    private static int getRandom(int num) {
        return random.nextInt(base.length());
    }

    private static Set<String> getRandomString(int len) {
        String[] tags = getZuhe(20);
        HashSet<String> set = new HashSet<>();
        for (int i = 0; i < len; i++) {
            int index = getRandom(tags.length);
//            System.out.print(tags[index] + ",");
            set.add(tags[index]);
        }
//        System.out.println(set.size());

        return set;
    }


    private static Set<String> getZuheNum(int len) {
        HashSet<String> set = new HashSet<>();
        for (int i = 0; i < len; i++) {
            set.add(random.nextInt(800) + "");
        }
        return set;
    }

    private static String[] getZuhe(int len) {
        String[] split = base.split("");
        HashSet<String> set = new HashSet<>();
        for (int i = 0; i < 20; i++) {
            set.add(split[i]);
        }
        for (int i = 0; i <= len; i++) {
            String s1 = split[i];
            for (int j = i + 1; j < split.length; j++) {
                String s2 = s1.concat(split[j]);
                set.add(s2);
            }
        }
        return String.join(",", set).split(",");


    }

    static class DataBeam {
        Set<String> tags;
        String id;
        double fx;
        double zx;

        @Override
        public String toString() {
            return id +
                    "," + fx +
                    "," + zx +
                    "," + tags;
        }

        public DataBeam(String id, double fx, double zx) {
            this.id = id;
            this.fx = fx;
            this.zx = zx;
        }

        public DataBeam(Set<String> tags, String id, double fx, double zx) {
            this.tags = tags;
            this.id = id;
            this.fx = fx;
            this.zx = zx;
        }

        public Set<String> getTags() {
            return tags;
        }

        public void setTags(Set<String> arr) {
            this.tags = arr;
        }

        public DataBeam setTags(String arr) {
            HashSet<String> set = new HashSet<>();
            for (String s : arr.split(",")) {
                set.add(s);
            }
            this.tags = set;
            return this;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public double getFx() {
            return fx;
        }

        public void setFx(double fx) {
            this.fx = fx;
        }

        public double getZx() {
            return zx;
        }

        public void setZx(double zx) {
            this.zx = zx;
        }

    }

}


