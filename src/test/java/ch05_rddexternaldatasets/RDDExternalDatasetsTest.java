package ch05_rddexternaldatasets;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RDDExternalDatasetsTest {

    private final SparkConf sparkConf = new SparkConf()
            .setAppName("Spark First Program")
            .setMaster("local[*]");

    // test loading local file into apache spark
    @ParameterizedTest
    @ValueSource(strings = {
            "src\\test\\resources\\1000words.txt",
            "src\\test\\resources\\wordslist.txt.gz",
    })
    public void testLoadingLocalTextFileIntoSparkRDD(final String localFilePath) {
        try(final var sparkContext = new JavaSparkContext(sparkConf)){
            final var myRdd = sparkContext.textFile(localFilePath);

            System.out.println("Total lines in the file: " + myRdd.count());
            // printing the first 10
            myRdd.take(10).forEach(System.out::println);
            System.out.println("++++++++++++++++++++++++++");
        }

    }


}
