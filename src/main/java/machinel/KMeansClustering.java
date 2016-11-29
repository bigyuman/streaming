package machinel;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.Arrays;

/**
 * Created by ajnebro on 16/11/15.
 */
public class KMeansClustering {
  static Logger log = Logger.getLogger(KMeansClustering.class.getName());

  public static void main(String[] args) {
    SparkConf conf;
    conf = new SparkConf().setAppName("K-means Example");
    JavaSparkContext sc = new JavaSparkContext(conf);

    if (args.length != 1) {
      log.fatal("Syntax Error: data file missing");
      throw new RuntimeException();
    }
    String path = args[0];

    // Load and parse data
    JavaRDD<String> data = sc.textFile(path);
    data.cache();
    JavaRDD<Vector> parsedData = data.map(
            s -> {
              String[] sarray = s.split(" ");
              double[] values = new double[sarray.length];
              for (int i = 0; i < sarray.length; i++)
                values[i] = Double.parseDouble(sarray[i]);
              return Vectors.dense(values);
            }
    );


        /*
    JavaRDD<Vector> parsedData = data.map(
            s -> Vectors.dense(Arrays.stream(s.split(" "))
                .mapToDouble(Double::parseDouble)
                .toArray()));
*/
    parsedData.cache();

    // Cluster the data into two classes using KMeans
    int numClusters = 2;
    int numIterations = 20;
    KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

    // Save and load model
    clusters.save(sc.sc(), "kmeansModelPath");
    //KMeansModel sameModel = KMeansModel.load(sc.sc(), "kmeansModelPath");
  }

}
