package com.jason.example;


import java.io.*;

public class Test {
    private static String replacex(String s) {
        if (null == s) {
            throw new NullPointerException("传入的数据不能为null");
        }
        return s.replaceAll("\t", " ");
    }

    public static void main(String[] args) throws IOException {
        try (
                BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("aa"), "utf-8"));
                PrintWriter printWriter = new PrintWriter("bb")
        ) {
            String s = null;
            while ((s = reader.readLine()) != null) {
                printWriter.println(replacex(s));
            }
        }


    }
}
