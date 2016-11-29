package bigyu.master.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.List;

import scala.Tuple2;

/**
 * Program that counts the number of Spanish airports per type, taken from: http://ourairports.com/data/
 *
 * @author Antonio J. Nebro <antonio@lcc.uma.es>
 */

public class SparkAirport {
	public static void main(String[] args) {

		// STEP 1: argument checking
		if (args.length == 0) {
			throw new RuntimeException("The number of args is 0. Usage: "
					+ "SparkAirport directory") ;
		}

		// STEP 2: create a SparkConf object
		SparkConf conf = new SparkConf().setAppName("SparkAirport") ;

		// STEP 3: create a Java Spark context
		JavaSparkContext sparkContext = new JavaSparkContext(conf) ;

		// STEP 4: read the lines from the file(s)
		JavaRDD<String> lines = sparkContext.textFile(args[0]) ;

		// STEP 5: map the string coded numbers into integers
		JavaRDD<String> spanishAirports;
		spanishAirports = lines.filter(new Function<String, Boolean>() {
			@Override public Boolean call(String s) throws Exception {
				Boolean foundAirport = false ;

        String[] airportFields = s.split(",") ;

        if (airportFields[8].equals("\"ES\"")) {
          foundAirport = true ;
        }

				return foundAirport ;
			}
		}) ;


    JavaPairRDD<String, Integer> groups = spanishAirports.mapToPair(
        new PairFunction<String, String, Integer>() {
          public Tuple2<String, Integer> call(String string) {
            String[] airportFields = string.split(",") ;

            String airportType = airportFields[2] ;
            return new Tuple2<String, Integer>(airportType, 1);
          }
        }
    );


    JavaPairRDD<String, Integer> counts = groups.reduceByKey(
        new Function2<Integer, Integer, Integer>() {
          public Integer call(Integer integer, Integer integer2) throws Exception {
            return integer + integer2 ;
          }
        }
    ) ;

    // STEP 7: sort the results by key
    List<Tuple2<String, Integer>> output = counts.sortByKey().collect() ;

    System.out.println("Result: " + output.size()) ;

    // STEP 8: print the results
    for (Tuple2<?, ?> tuple : output) {
      System.out.println(tuple._1() + ": " + tuple._2()) ;
    }

		// STEP 8: stop de spark context
		sparkContext.stop();  
	}
}
