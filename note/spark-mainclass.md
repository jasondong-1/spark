主要介绍了我们写的spark代码是如何执行的，源代码点击[这里](https://github.com/jasondong-1/spark/blob/master/spark-exaples/src/main/scala/com/jason/exaples/MysqlExample.scala)  

#  SparkSessiond的创建  
```
  val spark = SparkSession
    .builder()  //此处调用了new Builder  
    .appName("test") // 设置app名字
    .master("local[1]")// 设置master
    .config("aa","bb")//添加配置项，可以通过获得sparkconf来获取该kv对，master，appname方法也是调用的该方法
    .getOrCreate() // 创建sparkSession
```

#  创建DataFrame  
我们以读取csv文件为例来说明  
```
    val csvdf = spark
      .read // new DataFrameReader(spark)
      .format("csv") //设置输入数据类型
      .option("header", "true") // 有关csv的一些配置
      .option("sep", "\t")
      .option("inferSchema", "false")
      .schema("name string, age int")//调用了StructType.fromDDL(schemaString) csv,json等可以根据数据推断schema，此方法可以避免推断，节省时间 
      .load("data/person.csv") 
```
## load方法最总创建出了DataFrame  

### load方法_part1  
```
    //1.   
    if (source.toLowerCase(Locale.ROOT) == DDLUtils.HIVE_PROVIDER) {
      throw new AnalysisException("Hive data source can only be used with tables, you can not " +
        "read files of Hive data source directly.")
    }
    //2.
    val cls = DataSource.lookupDataSource(source, sparkSession.sessionState.conf)
```
line1:不接受format（"hive"）,否则跑出异常
line2：通过format（“source”）中的source来找到对应的类（DataSourceRegister子类），原理：  
>1.通过classloader 加载出所有的Iterator[DataSourceRegister]，然后遍历比对shortname与source  
2.走不通，则直接加载 “source”类 或者 “source.DefaultSource”

source与类的对应示例：  
csv -> org.apache.spark.sql.execution.datasources.csv.CSVFileFormat  
jdbc -> org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider    

### load方法_part2  
```
if (classOf[DataSourceV2].isAssignableFrom(cls)) {
...
}else{
 loadV1Source(paths: _*)
}
```
加载csv没走if，直接走的else，else直接调用了loadV1Source(paths: _*)，先看这个方法  


### loadV1Source  
```
  private def loadV1Source(paths: String*) = {
    // Code path for data source v1.
    sparkSession.baseRelationToDataFrame(
      DataSource.apply(
        sparkSession,
        paths = paths,
        userSpecifiedSchema = userSpecifiedSchema,
        className = source,
        options = extraOptions.toMap).resolveRelation())
  }
```

loadV!SOurce(paths: Stirng*) 通过调用SparkSession#baseRelationToDataFrame(baseRelation: BaseRelation)方法创建了DataFrame
```
def baseRelationToDataFrame(baseRelation: BaseRelation): DataFrame = {
    Dataset.ofRows(self, LogicalRelation(baseRelation))
  }
```   

####  baseRelationToDataFrame(baseRelation: BaseRelation)中的baseRelation是如何获得的  
DataSource#resolveRelation  

该方法通过给定的source（“jdbc”，“csv”等等）找到对应的provider，我们这里找到的是JDBCRelationProvider,然后调用provider的  
createRelation 方法创建Relation  

### Dataset#ofRows    
Dataset.ofRows(self, LogicalRelation(baseRelation))  
```
  def ofRows(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame = {
    val qe = sparkSession.sessionState.executePlan(logicalPlan)
    qe.assertAnalyzed()
    new Dataset[Row](sparkSession, qe, RowEncoder(qe.analyzed.schema))
  }
```
ofRows方法最终创建了DataFrame（Dataset[Row]）   