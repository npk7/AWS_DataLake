
def create_spark_session():
    """
    this function creates spark session and returns it
    
    INPUTS
        none
    OUTPUT
        spark session
    """



def process_song_data():
    """
    this function processes song data from the input file.
    
    INPUTS
        spark - spark context
        input_data - path to input data
        output_data - path to output parquet files
    
    the function reads data from S3, processes in HDFS, and attempt to write back to S3
    
    """
    


def process_log_data():
    """
    this function processes log data from the input file.
    
    INPUTS
        spark - spark context
        input_data - path to input data
        output_data - path to output parquet files
    
    the function reads data from S3, processes in HDFS, and attempt to write back to S3
    
    """
    
    

def main():
    spark = create_spark_session()
    input_data = "s3a://thebucket-dend/"
    output_data = "data"
    
    process_song_data()    
    process_log_data()


if __name__ == "__main__":
    main()
