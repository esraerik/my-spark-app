
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;










import org.apache.spark.ml.feature.StopWordsRemover;




//import WordCountJava.SerializableComparator;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Scanner;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public class WordCount {
  private static final FlatMapFunction<String, String> WORDS_EXTRACTOR =
      new FlatMapFunction<String, String>() {
        public Iterable<String> call(String s) throws Exception {
          return Arrays.asList(s.split(" "));
        }
      };

  private static final PairFunction<String, String, Integer> WORDS_MAPPER =
      new PairFunction<String, String, Integer>() {
        public Tuple2<String, Integer> call(String s) throws Exception {
          return new Tuple2<String, Integer>(s, 1);
        }
      };

  private static final Function2<Integer, Integer, Integer> WORDS_REDUCER =
      new Function2<Integer, Integer, Integer>() {
        public Integer call(Integer a, Integer b) throws Exception {
          return a + b;
        }
      };

  public static void main(String[] args) throws IOException {
	  System.setProperty("hadoop.home.dir", "C:\\winutil\\");
    String test = new String();
	if (args.length < 1) {
      test="src/test/resources/kitap.txt";
      //System.err.println("Please provide the input file full path as argument");
      //System.exit(0);
    }
	 List<String> stopWords;

     // Load stop words
     try (FileReader inputStream = new FileReader("src/test/resources/stopWords.txt");
          BufferedReader bufferedReader = new BufferedReader(inputStream)) {
        stopWords = bufferedReader.lines().collect(Collectors.toList());
     }
   
    	  System.out.println(stopWords.size()+stopWords.get(45));
	
    SparkConf conf = new SparkConf().setAppName("org.sparkexample.WordCount").setMaster("local");
    JavaSparkContext context = new JavaSparkContext(conf);
    
    JavaRDD<String> file = context.textFile(test);
    JavaRDD<String> words = file.flatMap(WORDS_EXTRACTOR);
    JavaRDD<String> counter3= words.filter(s -> !stopWords.contains(s));
    JavaPairRDD<String, Integer> pairs = counter3.mapToPair(WORDS_MAPPER);
    JavaPairRDD<String, Integer> counter = pairs.reduceByKey(WORDS_REDUCER);
    List<Tuple2<String, Integer>> counter2= counter.takeOrdered(50, (SerializableComparator<Tuple2<String, Integer>>) (o1, o2) -> o2._2().compareTo(o1._2()));
   
    counter2.forEach(res -> System.out.format("'%s' appears %d times\n", res._1(), res._2()));
    FileWriter fw = new FileWriter("src/test/resources/count/sonuc.txt");
	BufferedWriter bw = new BufferedWriter(fw);
	for (int i = 0; i < counter2.size(); i++) {
		bw.write(counter2.get(i).toString());
		bw.newLine();
	}
	
	bw.close();
//    counter2.saveAsTextFile("src/test/resources/count");
  }
  
  private String getFile(String fileName) {

	StringBuilder result = new StringBuilder("");

	//Get file from resources folder
	ClassLoader classLoader = getClass().getClassLoader();
	File file = new File(classLoader.getResource(fileName).getFile());

	try{
		Scanner scanner = new Scanner(file);
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			result.append(line).append("\n");
		}

		scanner.close();

	} catch (IOException e) {
		e.printStackTrace();
	}
		
	return result.toString();

  }
  private interface SerializableComparator<T> extends Comparator<T>, Serializable {
      int compare(T o1, T o2);
   }
}
