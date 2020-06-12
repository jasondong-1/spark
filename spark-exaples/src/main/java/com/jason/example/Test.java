package com.jason.example;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class Test {
    private static String replacex(String s) {
        if (null == s) {
            throw new NullPointerException("传入的数据不能为null");
        }
        return s.replaceAll("\t", " ");
    }

    public static void tdengine() throws ClassNotFoundException, SQLException {
        Class.forName("com.taosdata.jdbc.TSDBDriver");
        Connection conn = null;
        try {
            conn = DriverManager.getConnection("jdbc:TAOS://192.168.1.151:6030/test", "root", "taosdata");
            PreparedStatement prst = conn.prepareStatement("DESCRIBE log");
            ResultSet rs = prst.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            int cnt = meta.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= cnt; i++) {
                    System.out.println(rs.getString(i));
                }
            }
        } finally {
            conn.close();
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException, SQLException, ClassNotFoundException {
        /*try (
                BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("aa"), "utf-8"));
                PrintWriter printWriter = new PrintWriter("bb")
        ) {
            String s = null;
            while ((s = reader.readLine()) != null) {
                printWriter.println(replacex(s));
            }
        }*/
        /*System.out.println(UUID.randomUUID().getLeastSignificantBits());
        System.out.println(Math.abs(UUID.randomUUID().getLeastSignificantBits()));
        ConcurrentHashMap<String,String> map = new ConcurrentHashMap<>(3);*/
        /*System.out.println(System.getProperty("java.class.path"));*/
        // System.out.println(System.getProperty("user.dir"));
        tdengine();
    }
}
