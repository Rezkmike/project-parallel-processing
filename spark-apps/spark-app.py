from pyspark.sql import SparkSession

def main():
    # Create a Spark session
    spark = SparkSession.builder \
        .appName("WordCount") \
        .getOrCreate()

    # Specify the path to the input file (can be adjusted for your use case)
    input_file = "hdfs://path/to/input.txt"  # Replace with your file path
    output_dir = "hdfs://path/to/output"    # Replace with your desired output directory

    # Read the input file into an RDD
    text_rdd = spark.sparkContext.textFile(input_file)

    # Perform the word count operation
    word_counts = text_rdd \
        .flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)

    # Save the result to the specified output directory
    word_counts.saveAsTextFile(output_dir)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
