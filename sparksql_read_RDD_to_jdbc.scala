package SparkSql
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.{DataFrame,SaveMode}
import org.apache.spark.sql.DataFrameReader
import java.util.Properties

object sparksql_read_to_jdbc {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "e:/Program Files (x86)/hadoop2.6");
    //初始化配置：设置主机名和程序主类的名字
    val conf = new SparkConf().setMaster("local").setAppName("jsonfile")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //遇到问题：
    /*
     * 在第一次远程访问mysql的时候，需要事先授予相应的权限；
     * （1）你想root使用123456从任何主机连接到mysql服务器；
     * mysql>GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY '123456' WITH GRANT OPTION;
     * （2）如果你想允许用户jack从ip为10.10.50.127的主机连接到mysql服务器，并使用654321作为密码;
     * mysql>GRANT ALL PRIVILEGES ON *.* TO 'jack'@’10.10.50.127’ IDENTIFIED BY '654321' WITH GRANT OPTION;
     * mysql>FLUSH RIVILEGES;
     */

    val reader = sqlContext.read.format("jdbc")
    reader.option("url", "jdbc:mysql://192.168.230.129:3306/books")
    reader.option("driver", "com.mysql.jdbc.Driver")
    reader.option("user", "root")
    reader.option("password", "123456")
    reader.option("dbtable", "tb_book")
    val books = reader.load() //读取mysql表创建df

    books.registerTempTable("tb_book")
    val result = sqlContext.sql("select * from tb_book")
    result.show()

    //遇到的问题properties属性是java的包，
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "123456")
    //表里没有result这个表，可以自动创建
    result.write.mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.230.129:3306/books", "result", properties)
    
    sc.stop()
  }
}
