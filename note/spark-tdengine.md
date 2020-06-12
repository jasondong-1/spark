# spark + TDengine的使用  



## 企业介绍：

中国电信上海理想信息产业（集团）有限公司，成立于1999年，注册资本7000万元，是上海市投资规模较大的信息技术企业之一。母公司员工500多人，其中80％以上员工具有大学本科以上学历，从事软件开发人员超过50％，是一个典型的“知识密集型”企业。

通过整合公司内各事业部多年大型项目实施的整体实力，公司着力锻造大型信息化项目咨询规划和顶层设计能力，构建“智慧社区”、“智慧园区”及“智慧政务”、“智慧医疗”、“智慧物流”等各类智慧行业应用等整体解决方案，可提供IT外包服务和网络监控运维管理一站式安全解决方案，逐步形成“智慧城市”专业领域产品研发积累和项目交付与平台运营经验，锻造了整体科研队伍和项目实施团队的综合实力。

我们致力于中国IT产业发展，借助中国电信精品网络资源，定位于电信与IT产业融合的ICT服务商形象（ICT即“Information Communication Technology”），为社会信息化、企业信息化和家庭信息化提供全方位、专业化的应用集成服务。

可以参考网址：http://www.ideal.sh.cn/public/idealout/contentPreviewLinkDetail.htm?param=gsgk&pageCode=qyjs



## 项目介绍：

数据总线平台是基于spark+spring+Mybatis体系而开发的一个集ETL、智能调度功能为一体的互联网操作平台,平台以工业连接为基础，构建在安全可信的天翼云上，是可灵活扩展的工业互联网和工业大数据平台。  

该平台主要功能如下：  

1.提供基于hdfs、tdengine、hive、mysql、oralce、ftp等十余种数据源的快速数据加载  

2.对数据进行kv2table，table2kv，缺失值替换，增加序号列，过滤，类型转换，sql，机器学习模型计算等多种数据处理  

3.将处理好的数据存入hdfs、tdengine、hive、mysql、oralce、ftp等目标组件   

该平台通过web界面来对组件进行配置，基本摆脱了代码的编写，不会写代码的工作人员也可进行开发

![avatar](http://baidu.com/pic/doge.png)



因公司业务需要，最近需要用到spark+TDengine,下面简单记录一下使用的过程。  

## 1.tdengine的安装  
请参考官方[文档](https://www.taosdata.com/cn/documentation/)  
## 2.在tdengine中建立测试库和测试表  
```bash
taos> create database test;
taos>use test;
#这里我们创建一个和tdengine自带库log中log表结构一致的表，后提直接从log.log读数据存储到test.log_cp
taos> create table log_cp(
   ->  ts TIMESTAMP,
   ->  level TINYINT,
   ->  content BINARY(80),
   ->  ipaddr BINARY(15)
   -> )
```
## 3.spark 读取tdengine  
因为tdengine并未提供供spark调用的DataSource,而且tdengine本身也支持jdbc，所以这里使用了spark-jdbc  
来读取tdengine，最新的jdbc可以到[官网](https://www.taosdata.com/cn/documentation/connector/#Java-Connector)下载,我这里用的是如下版本：  
```xml
    <dependency>
        <groupId>com.taosdata.jdbc</groupId>
        <artifactId>taos-jdbcdriver</artifactId>
        <version>1.0.3</version>
    </dependency>
```
关于使用jdbc，官网有如下提示：  
>由于 TDengine 是使用 c 语言开发的，使用 taos-jdbcdriver 驱动包时需要依赖系统对应的本地函数库。    
 1.libtaos.so 在 linux 系统中成功安装 TDengine 后，依赖的本地函数库 libtaos.so 文件会被自动拷贝至 /usr/lib/libtaos.so，该目录包含在 Linux 自动扫描路径上，无需单独指定。  
 2.taos.dll 在 windows 系统中安装完客户端之后，驱动包依赖的 taos.dll 文件会自动拷贝到系统默认搜索路径 C:/Windows/System32 下，同样无需要单独指定。  

第一次使用时，为了保证机器上有libtaos.so 或 taos.dll，需要在本地安装tdengine客户端([win客户端](https://www.taosdata.com/cn/documentation/connector/#Windows%E5%AE%A2%E6%88%B7%E7%AB%AF%E5%8F%8A%E7%A8%8B%E5%BA%8F%E6%8E%A5%E5%8F%A3),[linux客户端](https://www.taosdata.com/cn/getting-started/#%E5%BF%AB%E9%80%9F%E4%B8%8A%E6%89%8B))  
spark的读取代码如下：  
```scala
    val jdbccdf = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:TAOS://192.168.1.151:6030/log")
      .option("driver", "com.taosdata.jdbc.TSDBDriver")
      .option("dbtable", "log")
      .option("user", "root")
      .option("password", "taosdata")
      .option("fetchsize", "1000")
      .load()
```

## 4.spark 存tdengine  
因为在读tdengine的时候，第一个字段ts会被转换为decimal，但是存储时直接存decimal tdengine是不认的，  
所以需要将ts进行类型转换  
```scala
jdbccdf.select(($"ts" / 1000000).cast(TimestampType).as("ts"), $"level", $"content", $"ipaddr")
      .write.format("jdbc")
      .option("url", "jdbc:TAOS://192.168.1.151:6030/test?charset=UTF-8&locale=en_US.UTF-8")
      .option("driver", "com.taosdata.jdbc.TSDBDriver")
      .option("dbtable", "log2")
      .option("user", "root")
      .option("password", "taosdata")
      .mode(SaveMode.Append)
      .save()
```
## 5.spark yarn 模式运行tdengine
上面的测试都是基于maser 为local测试的，如果以yarn模式运行，则在每个节点上都安装tdengineclient那是不现实的，  
查看taos-jdbcdriver的代码，发现，driver会执行System.load("taos"),也就是说只要java.library.path 中存在  
libtaos.so，程序就可正常运行，不必安装tdengine的客户端，因为java.library.path是在jvm启动时候就设置好的，要  
更改它的值，可以采用动态加载，采用如下方法  
解决了加载libtaos.so的问题  
1.将driver端libtaos.so发送到各个executor  
```scala
spark.sparkContext.addFile("/path/to/libtaos.so")  
```
2.重写spark 中JdbcUtils类中的createConnectionFactory方法 ，添加loadLibrary(new File(SparkFiles.get("libtaos.so")).getParent)  
进行java.library.path的动态加载    
```scala
 def createConnectionFactory(options: JDBCOptions): () => Connection = {
    val driverClass: String = options.driverClass
    () => {
      loadLibrary(new File(SparkFiles.get("libtaos.so")).getParent)
      DriverRegistry.register(driverClass)
      val driver: Driver = DriverManager.getDrivers.asScala.collectFirst {
        case d: DriverWrapper if d.wrapped.getClass.getCanonicalName == driverClass => d
        case d if d.getClass.getCanonicalName == driverClass => d
      }.getOrElse {
        throw new IllegalStateException(
          s"Did not find registered driver with class $driverClass")
      }
      driver.connect(options.url, options.asConnectionProperties)
    }
  }

```

3.loadLibrary方法如下  
```scala
  def loadLibrary(libPath: String): Unit = {
    var lib = System.getProperty("java.library.path")
    val dirs = lib.split(":")
    if (!dirs.contains(libPath)) {
      lib = lib + s":${libPath}"
      System.setProperty("java.library.path", lib)
      val fieldSysPath = classOf[ClassLoader].getDeclaredField("sys_paths")
      fieldSysPath.setAccessible(true)
      fieldSysPath.set(null, null)
    }
  }
```
在yarn 模式下一定要给url设置 charset 和 locale ，如charset=UTF-8&locale=en_US.UTF-8，否则container可能会异常退出  

## 6.libtaos.so 其他加载方式  
本来还尝试了jna加载libtaos.so的方式，此方式只需将libtaos.so 放入项目resources 中，程序变回自动搜索到so文件，奈何  
不会改tdengine中c的代码  



## 作者介绍：

董鸿飞，大数据开发工程师，2015年加入上海理想大数据实施部，工作至今。目前主要负责公司数据总线产品设计和开发。