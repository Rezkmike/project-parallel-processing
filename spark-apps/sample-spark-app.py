from pyspark.sql import SparkSession

def main(input_path, output_path):
    # Create a Spark session
    spark = SparkSession.builder.appName("Sample Spark App").getOrCreate()
    
    # Read input data
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    # Perform a transformation
    transformed_df = df.groupBy("column_name").count()  # Replace with your column name
    
    # Write the output
    transformed_df.write.csv(output_path, mode="overwrite", header=True)
    
    spark.stop()

if __name__ == "__main__":
    import sys
    main(sys.argv[1], sys.argv[2])
