import os
import uuid
from pyspark.sql import DataFrame

def save_temp_log(df: DataFrame, spark_session):
    base_path_D = "D:\\tmp\\hive"
    base_path_C = "C:\\tmp\\hive"

    # Use D: if it exists, otherwise fallback to C:
    base_path = base_path_D if os.path.exists(base_path_D) else base_path_C
    #os.makedirs(base_path, exist_ok=True)

    # Generate a unique folder name using UUID
    folder_name = f"log_{uuid.uuid4().hex}"
    full_path = os.path.join(base_path, folder_name)
    df.write.mode("overwrite").parquet(full_path)

    # Clear the cache
    try:
        print("Unpersisting DataFrame and clearing cache...")
        df.unpersist()              # If the DataFrame is cached, unpersist it
    except:
        print("No DataFrame to unpersist or already cleared.")
        pass
    
    spark_session.catalog.clearCache()

    df_final = spark_session.read.parquet(full_path)
    
    return df_final