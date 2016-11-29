package bigyu.master.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.List;

import scala.Tuple2;

/**
 * Program that counts the number of Spanish airports per type, taken from:
 * http://ourairports.com/data/. The data files are stored in a directory from
 * where they are read in streaming.
 *
 * @author Antonio J. Nebro <antonio@lcc.uma.es>
 */

public class SparkStreamingAirport {
  public static void main(String[] args) throws InterruptedException {

    // STEP 1: argument checking
    if (args.length == 0) {
      throw new RuntimeException("The number of args is 0. Usage: "
              + "SparkAirport directory");
    }

    // STEP 2: create a SparkConf object
    SparkConf conf = new SparkConf().setAppName("SparkStreamingAirport");

    // STEP 3: create a Java Spark context with a 2 seconds batch size
    JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(2));

    // STEP 4: read the lines from the file(s)
    JavaDStream<String> lines = streamingContext.textFileStream(args[0]);

    // STEP 5: data processing

    // Split each line into words
    JavaDStream<String> spanishAirports = lines.filter((Function<String, Boolean>) new Function<String, Boolean>() {
      @Override
      public Boolean call(String s) throws Exception {
        Boolean foundAirport = false;

        String[] airportFields = s.split(",");

        if (airportFields[8].equals("\"ES\"")) {
          foundAirport = true;
        }

        return foundAirport;
      }
    });

    JavaPairDStream<String, Integer>  groups = spanishAirports.mapToPair(
            string -> new Tuple2<>(string.split(",")[2], 1)) ;

    JavaPairDStream<String, Integer> output = groups
            .reduceByKey((integer, integer2) -> integer + integer2) ;

    output.print();
    streamingContext.start();
    streamingContext.awaitTermination();

    // STEP 7: stop de spark context
    streamingContext.stop();
  }
}
