package com.jason.example;

import java.io.IOException;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class ListZip {


    public static void main(String[] args) throws IOException {
        ZipFile zf = new ZipFile("spark-exaples-1.0-SNAPSHOT.jar");
        Enumeration e = zf.entries();
        while (e.hasMoreElements()) {
            ZipEntry ze2 = (ZipEntry) e.nextElement();
            System.out.println("File: " + ze2);
        }
    }
}
