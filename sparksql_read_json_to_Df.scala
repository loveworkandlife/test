package SparkSql
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.api.java.JavaRDD

object sparksql_read_RDD {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "e:/Program Files (x86)/hadoop2.6");
    //初始化配置：设置主机名和程序主类的名字
    val conf = new SparkConf().setMaster("local").setAppName("jsonfile")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val name = sc.makeRDD(Array(
      "{'name':'zhangsan','age':18}",
      "{'name':'lisi','age':19}",
       "{'name':'wangwu','age':21}",
      "{'name':'zhaojie','age':20}"))

    val score = sc.makeRDD(Array(
      "{'name':'zhangsan','score':56}",
      "{'name':'lisi','score':57}",
      "{'name':'wangwu','score':58}"))

    val namedf = sqlContext.read.json(name)
    val scoredf = sqlContext.read.json(score)
    //临时表只是一个指针，指向这个文件E:/jsonTest.txt。
    namedf.registerTempTable("table1")
    scoredf.registerTempTable("table2")
    val result = sqlContext.sql("select table1.name,table1.age,table2.score "
        +"from table1,table2 where table1.name=table2.name ")

    result.show()
    sc.stop()
  }
}
