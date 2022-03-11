package com.dorr.pingan.spark.tags;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 */
public class HiveTest {

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "dorr");
//        loadData2Hive();

        select();
    }

    public static void select() {
        Connection connection = getConnection();
        String sql = "SELECT * FROM user_tags";
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getString(1) + "\t" + rs.getDouble(2));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public HiveTest() {
    }

    /**
     * 将数据上传到hdfs中，用于load到hive表中，设置分隔符是","
     */
    public static void createFile(String dst, List<List<String>> argList) {
        try (FileSystem fileSystem = FileSystem.get(new URI("hdfs://localhost:8020"), new Configuration(), "dorr");
             FSDataOutputStream outputStream = fileSystem.create(new Path(dst))) {
            StringBuilder sb = new StringBuilder();
            for (List<String> arg : argList) {
                for (String value : arg) {
                    // 分隔符要和建表时指定的分隔符相同，否则数据能插入，但是后续查询的结果为空
                    sb.append(value).append(",");
                }
                sb.deleteCharAt(sb.length() - 1);
                sb.append("\n");
            }
            sb.deleteCharAt(sb.length() - 1);
            byte[] contents = sb.toString().getBytes();
            outputStream.write(contents);
            System.out.println("文件创建成功！");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 将HDFS文件load到hive表中
     */
    public static void loadData2Hive() {
        String sql = " insert into user_tags partition(dt='201211') values('802_803_804_805',10,30)";
        try (Connection connection = getConnection(); PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.execute();
            System.out.println("loadData到Hive表成功！");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static Connection getConnection() {
        String driverName = "org.apache.hive.jdbc.HiveDriver";
        String connectionUrl = "jdbc:hive2://localhost:10000/default";
        // 查询的时候不用用户名和密码也可以
        // 插入数据时必须要
        String username = "dorr";
        String password = "123456";
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(connectionUrl, username, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

}
