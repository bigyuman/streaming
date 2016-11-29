package machinel;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;

/**
 * Created by ajnebro on 16/11/15.
 */
public class KMeansClassification {
  static Logger log = Logger.getLogger(KMeansClassification.class.getName());

  public static void main(String[] args) {
    SparkConf conf;
    conf = new SparkConf().setAppName("K-means Example");
    JavaSparkContext sc = new JavaSparkContext(conf);

    if (args.length != 1) {
      log.fatal("Syntax Error: directory with the model missing")  ;
      throw new RuntimeException();
    }
    String path = args[0] ;

    KMeansModel model = KMeansModel.load(sc.sc(), path);

    Vector v = new DenseVector(new double[]{1.1, 3.2}) ;
    System.out.println("Vector 1: " + v) ;
    System.out.println("Data 1: " + model.predict(v)) ;
    v = new DenseVector(new double[]{5.1, 1.4}) ;
    System.out.println("Vector 2: " + v) ;
    System.out.println("Data 2: " + model.predict(v)) ;
    v = new DenseVector(new double[]{5.2, 2}) ;
    System.out.println("Vector 3: " + v) ;
    System.out.println("Data 3: " + model.predict(v)) ;
    v = new DenseVector(new double[]{1, 4}) ;
    System.out.println("Vector 4: " + v) ;
    System.out.println("Data 4: " + model.predict(v)) ;
  }
}
