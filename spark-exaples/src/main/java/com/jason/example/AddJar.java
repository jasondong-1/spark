package com.jason.example;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

public class AddJar {
    public static void main(String[] args)
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException,
            SecurityException, ClassNotFoundException, InstantiationException, MalformedURLException {
//         String path =
//         "/xx/.m2/repository/org/apache/commons/commons-lang3/3.1/commons-lang3-3.1.jar";
        //String path = "commons-lang3-3.1.jar";//jar文件需放在工程目录下
        String path = "udf.jar";//jar文件需放在工程目录下

        loadJar(path);
        /*Class<?> aClass = Class.forName("org.apache.commons.lang3.StringUtils");
        Object instance = aClass.newInstance();
        Object strip = aClass.getDeclaredMethod("strip", String.class, String.class).invoke(instance,
                "[1,2,3,4,5,6,2,3,5,1]", "[]");
        System.out.println(strip);*/

        System.setProperty("java.class.path",System.getProperty("java.class.path")+":udf.jar");
        System.out.println(System.getProperty("java.class.path"));


    }

    private static void loadJar(String jarPath) throws MalformedURLException {
        File jarFile = new File(jarPath); // 从URLClassLoader类中获取类所在文件夹的方法，jar也可以认为是一个文件夹

        if (jarFile.exists() == false) {
            System.out.println("jar file not found.");
            return;
        }

        //获取类加载器的addURL方法，准备动态调用
        Method method = null;
        try {
            method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
        } catch (NoSuchMethodException | SecurityException e1) {
            e1.printStackTrace();
        }

        // 获取方法的访问权限，保存原始值
        boolean accessible = method.isAccessible();
        try {
            //修改访问权限为可写
            if (accessible == false) {
                method.setAccessible(true);
            }

            // 获取系统类加载器
            URLClassLoader classLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();

            //获取jar文件的url路径
            java.net.URL url = jarFile.toURI().toURL();

            //jar路径加入到系统url路径里
            method.invoke(classLoader, url);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //回写访问权限
            method.setAccessible(accessible);
        }

    }
}
