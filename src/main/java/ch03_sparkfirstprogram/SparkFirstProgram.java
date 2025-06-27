package ch03_sparkfirstprogram;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SparkFirstProgram {

    public static void main(String[] args) {

        try(
                SparkSession sparkSession = SparkSession
                    .builder()
                    .appName("Spark First Program")
                    .master("local[*]")
                    .getOrCreate();

                var javaSparkContext =  new JavaSparkContext(sparkSession.sparkContext());
        ){

            // creating an rdd

            List<Integer> data= Stream
                    .iterate(1, x->x+1)
                    .limit(5)
                    .collect(Collectors.toList());

            JavaRDD<Integer> myjavaRdd =  javaSparkContext.parallelize(data);

            // number of elements in the rdd
            System.out.println("Number of elements in the rdd: " + myjavaRdd.count());

            // get the number of partitions
            System.out.println("Number of partitions: " + myjavaRdd.getNumPartitions());

            // some transformations and actions

            // one important one is reduce - cos we are reducing it to a single number / output
                // max, min, sum

            Integer max = myjavaRdd.reduce(Integer::max);
            System.out.println("Max: " + max);

            Integer min = myjavaRdd.reduce(Integer::min);
            System.out.println("Min: " + min);

            Integer sum = myjavaRdd.reduce(Integer::sum);
            System.out.println("Sum: " + sum);


            // visualising in the web ui - we are using the scanner as a mechanism to wait so that the application do not terminate
            try(final var scanner = new java.util.Scanner(System.in)) {
                scanner.nextLine();
            }
        }
        /*
        > for an actual actual cluster, like aws emr, spark will use the
           spark-submit command to submit the job to the cluster

        > for a local cluster, like a laptop, spark will use the

        */






    }
}
