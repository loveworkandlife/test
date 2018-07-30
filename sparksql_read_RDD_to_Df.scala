package SparkSql
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.api.java.JavaRDD

object sparksql_learn {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "e:/Program Files (x86)/hadoop2.6");
    //初始化配置：设置主机名和程序主类的名字
    
    val conf = new SparkConf().setMaster("local").setAppName("jsonfile")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    val df = sqlContext.read.json("E:/jsonTest.txt");
    df.show()
    df.printSchema()

   //临时表只是一个指针，指向这个文件E:/jsonTest.txt。
    df.registerTempTable("table1")
    val result=sqlContext.sql("select age,count(*) as count from table1 group by age")
    
    result.show()
    sc.stop()
  }
}
