
/* SimpleApp.java */
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class SimpleSpark {
  public static void main(String[] args) {
	System.setProperty("hadoop.home.dir", "C:\\winutil\\");
   // String logFile = "C:\\Users\\ESRA ERIK\\Desktop\\my-spark-app\\src\\test\\resources\\count\\sonuc.txt"; // Should be some file on your system
	String logFile = "src/test/resources/count/sonuc.txt"; // Should be some file on your system
	SparkConf conf = new SparkConf().setAppName("my-spark-app").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> logData = sc.textFile(logFile).cache();

    long numAs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("a"); }
    }).count();

    long numBs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("b"); }
    }).count();

    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
  }
}