package ch05_rddexternaldatasets;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.stream.Stream;

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
    @DisplayName("Testing loading local Textfiles into spark rdd using value source")
    public void testLoadingLocalTextFileIntoSparkRDD(final String localFilePath) {
        try(final var sparkContext = new JavaSparkContext(sparkConf)){
            final var myRdd = sparkContext.textFile(localFilePath);

            System.out.println("Total lines in the file: " + myRdd.count());
            // printing the first 10
            myRdd.take(10).forEach(System.out::println);
            System.out.println("++++++++++++++++++++++++++");
        }

    }

    // test using method source
    @ParameterizedTest
    @MethodSource("getFilePath")
    @DisplayName("Testing loading local Textfiles into spark rdd using method source")
    public void testLoadingLocalTextFileIntoSparkRDDMethodSource(final String localFilePath) {
        try(final var sparkContext = new JavaSparkContext(sparkConf)){
            final var myRdd = sparkContext.textFile(localFilePath);

            System.out.println("Total lines in the file: " + myRdd.count());
            // printing the first 10
            myRdd.take(10).forEach(System.out::println);
            System.out.println("++++++++++++++++++++++++++");
        }

    }




    private static Stream<Arguments> getFilePath(){

        return Stream.of(
                Arguments.of("src\\test\\resources\\1000words.txt"),
                Arguments.of("src\\test\\resources\\wordslist.txt.gz")
        );
    }



}
