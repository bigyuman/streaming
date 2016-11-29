package bigyu.master.streaming;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;
import java.util.List;

import scala.Tuple2;

/**
 * Program that process files from smassa, taken from:
 * http://datosabiertos.malaga.eu/dataset/ocupacion-aparcamientos-smassa.
 * The data files are stored in a directory from  where they are read in streaming.
 *
 * @author Juanjo Carmona
 */

public class Smassa {
	public static void main(String[] args) throws InterruptedException {
		
		Logger.getLogger("org").setLevel(Level.OFF);
		
	    // STEP 1: argument checking
	    if (args.length == 0) {
	      throw new RuntimeException("The number of args is 0. Usage: "
	              + "smassa files directory");
	    }

	    // STEP 2: create a SparkConf object
	    SparkConf conf = new SparkConf().setAppName("SparkStreamingSmassa");

	    // STEP 3: create a Java Spark context with a 2 seconds batch size
	    JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));

	    // STEP 4: read the lines from the file(s)
	    JavaDStream<String> lines = streamingContext.textFileStream(args[0]);

	    // STEP 5: data processing

	    // Split each line into words
	    JavaDStream<List<String>> smassaData = lines.map(
	    		line ->{
	    			String name = line.split(",")[1];
	    			String cap = line.split(",")[8];
	    			String time = line.split(",")[10];
	    			String free = line.split(",")[11];
	    		
	    		return Arrays.asList(name,cap,time,free);
	    		});
	    smassaData.cache();
	    smassaData.foreachRDD(rdd->{
	    	List<List<String>> datos = rdd.collect();
	    	for (List<String> v : datos){
	    		System.out.println("Name: "+v.get(0) + "  Capacity: " + v.get(1) + "   Time: " + v.get(2) + "   Free: "+v.get(3));
	    	}
	    });
	    
	    //smassaData.print();
	    streamingContext.start();
	    streamingContext.awaitTermination();

	    // STEP 7: stop de spark context
	    //streamingContext.stop();
	    
	    
	  }

}
