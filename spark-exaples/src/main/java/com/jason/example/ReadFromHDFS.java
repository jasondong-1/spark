package com.jason.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.spark_project.jetty.server.Authentication;

import javax.security.auth.login.LoginContext;
import java.io.IOException;

public class ReadFromHDFS {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();

        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");

        System.setProperty("KRB5CCNAME", "/data1/databus_open/ticket_files/4/paaskf_hadoop_instance/ys_user02");

        conf.set("KRB5CCNAME", "/data1/databus_open/ticket_files/4/paaskf_hadoop_instance/ys_user02");

        //UserGroupInformation.setConfiguration(conf);
        //UserGroupInformation.loginUserFromKeytab(conf.get("login.kerberos.principal"), conf.get("login.keytab.file"));

        // 此处需要更改为自己的 票据文件路径和 principal
        //UserGroupInformation.loginUserFromKeytab("principal", "path/to/keytab_file");
        //paas_ys_g@EXAMPLE.COM
        //UserGroupInformation ugi = UserGroupInformation.getUGIFromTicketCache("/data1/databus_open/ticket_files/4/paaskf_hadoop_instance/ys_user02",args[0]);
        //UserGroupInformation.getLoginUser().reloginFromTicketCache();
        //UserGroupInformation.loginUserFromKeytab(args[0],"/data1/databus_open/ticket_files/4/paaskf_hadoop_instance/ys_user02");
        //UserGroupInformation.isLoginTicketBased();
        FileSystem fs = FileSystem.get(conf);
        try {
            FileStatus[] fss = fs.listStatus(new Path("private"));
            for (FileStatus ft : fss) {
                System.out.println(ft.getPath().getName());
            }
        } finally {
            fs.close();
        }
        /*try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path("hdfs path"))))) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        } finally {
            fs.close();
        }*/

    }
}
