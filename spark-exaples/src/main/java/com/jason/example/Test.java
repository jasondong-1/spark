package com.jason.example;



import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class Test {
    private static String replacex(String s) {
        if (null == s) {
            throw new NullPointerException("传入的数据不能为null");
        }
        return s.replaceAll("\t", " ");
    }

    public static void main(String[] args) throws IOException, URISyntaxException {
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
        System.out.println(System.getProperty("user.dir"));
    }
}
