package com.jason.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;

public class CpTest {
    public void ff() throws IOException {
        File f = new File(this.getClass().getResource("/").getPath());

        System.out.println(f);

        // 获取当前类的所在工程路径; 如果不加“/”  获取当前类的加载目录  D:\git\daotie\daotie\target\classes\my
        File f2 = new File(this.getClass().getResource("").getPath());
        System.out.println(f2);

        // 第二种：获取项目路径    D:\git\daotie\daotie
        File directory = new File("");// 参数为空
        String courseFile = directory.getAbsolutePath();
        System.out.println(courseFile);
    }

    public static void main(String[] args) throws IOException {
        new CpTest().ff();
    }
}
