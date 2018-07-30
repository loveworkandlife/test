package SparkSql
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.{ DataFrame, SaveMode }
import org.apache.spark.sql.DataFrameReader
import java.util.Properties
import org.apache.spark.sql.RowFactory
import com.sun.xml.internal.ws.wsdl.writer.document.Import
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.{ StructField, StringType }

object sparksql_udf {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "e:/Program Files (x86)/hadoop2.6");
    //初始化配置：设置主机名和程序主类的名字
    val conf = new SparkConf().setMaster("local").setAppName("jsonfile")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val rdd = sc.makeRDD(Array("zhangsan", "lisi", "wangwu"))
    //   rdd.take(10).foreach { x => print(x) }
    val rowRDD = rdd.map {
      x => { RowFactory.create(x) }
    }

    rowRDD.take(10).foreach { x => print(x) }

    val schema = DataTypes.createStructType(Array(StructField("name", StringType, true)))
    val df = sqlContext.createDataFrame(rowRDD, schema)
    val books = df.registerTempTable("user")
    sqlContext.udf.register("StrLen", (s: String, i: Int) => { s.length() + i })
    sqlContext.sql("select name ,StrLen(name,5) as length from user").show

    /**
     * //遇到的问题properties属性是java的包，
     * val properties = new Properties()
     * properties.setProperty("user", "root")
     * properties.setProperty("password", "123456")
     * //表里没有result这个表，可以自动创建
     * result.write.mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.230.129:3306/books", "result", properties)
     */
    sc.stop()
  }
}
