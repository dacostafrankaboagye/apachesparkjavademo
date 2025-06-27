package ch03_sparkfirstprogram;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class CreateRDDUsingParallelizeTest {

    private final SparkConf sparkConf = new SparkConf()
            .setAppName("Spark First Program")
            .setMaster("local[*]");

    @Test
    @DisplayName("Create an empty rdd with no partition in spark")
    void createAnEmptyRDDWithNoPartitionsInSpark() {
        try(final var sparkContext = new JavaSparkContext(sparkConf)){
            final var emptyRdd = sparkContext.emptyRDD();
            assertEquals(0, emptyRdd.partitions().size());
        }
    }

    // with default partition
    @Test
    @DisplayName("Create an empty rdd with default partition in spark")
    void createAnEmptyRDDWithDefaultPartitionsInSpark() {
        try(final var sparkContext = new JavaSparkContext(sparkConf)){
            final var emptyRdd = sparkContext.parallelize(List.of());  // ParallelizeConnectionRDD[0]
            var myCpuCores = Runtime.getRuntime().availableProcessors();
            assertEquals(myCpuCores, emptyRdd.partitions().size());
        }
    }

    // create a sparkRDD from Java Collection
    @Test
    @DisplayName("Create a sparkRDD from Java Collection")
    void createASparkRDDFromJavaCollection() {
        try(final var sparkContext = new JavaSparkContext(sparkConf)){
            final var data = Stream.iterate(1, x->x+1).limit(8L).collect(Collectors.toList());
            final var myRdd = sparkContext.parallelize(data);
            assertEquals(8, myRdd.partitions().size());  // for my pc, it is 8, based on the "local[*]" which Run Spark locally with as many worker threads as logical cores on the machine.
            assertEquals(8, myRdd.count());
            assertEquals(1, myRdd.first());

        }
    }

    // give the nuber of partitions
    @Test
    @DisplayName("Create a sparkRDD from Java Collection with given number of partitions")
    void createASparkRDDFromJavaCollectionWithGivenNumberOfPartitions() {
        try(final var sparkContext = new JavaSparkContext(sparkConf)){
            final var data = Stream.iterate(1, x->x+1).limit(8L).collect(Collectors.toList());
            final var myRdd = sparkContext.parallelize(data, 2);  // we have specified the number of partitions
            assertEquals(2, myRdd.partitions().size()); // this now becomes 2
            assertEquals(8, myRdd.count());
            assertEquals(1, myRdd.first());

        }
    }

}