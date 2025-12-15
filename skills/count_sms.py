import os
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, when, expr, concat, lit, row_number, collect_list, concat_ws, trim, count, regexp_replace
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from web.pyspark_session import get_spark_session
from web.save_files import save_to_csv

def function_complete_SMS(input_folder, output_folder, partitions, Widget_Process):
    
    spark = get_spark_session()

    sqlContext = SQLContext(spark)

    files = []
    for root, _, file_names in os.walk(input_folder):
        for file_name in file_names:
            if file_name.endswith('.csv') or file_name.endswith('.csv'):
                files.append(os.path.join(root, file_name))
    
    consolidated_df = None

    for file in files:
        if file.endswith('.csv'):
            #df = spark.read.csv(file, header=True, inferSchema=True)
            df = spark.read.option("delimiter", ";").csv(file, header=True, inferSchema=True)
        elif file.endswith('.txt'):
            df = spark.read.option("delimiter", "\t").csv(file, header=True, inferSchema=True)

        if consolidated_df is None:
            consolidated_df = df
        else:
            consolidated_df = consolidated_df.unionByName(df, allowMissingColumns=True)

    if consolidated_df is not None:

        selected_columns = [
            "Cuenta_Next", "Cuenta", 
            "Edad_Mora"]
        
        consolidated_df = consolidated_df.select(*selected_columns)
        
        consolidated_df = consolidated_df.withColumn(
            "Cuenta",
            when(col("Cuenta").isNull() | (trim(col("Cuenta")) == ""), "0")
            .otherwise(col("Cuenta"))
        )

        consolidated_df = Function_Modify(consolidated_df)
        delimiter = ";"
        Type_Proccess = "Consolidado SMS"
        
        save_to_csv(consolidated_df, output_folder, Type_Proccess, partitions, delimiter)

    else:

        spark.stop()

def Function_Modify(RDD):
    Data_Frame = RDD 
    Data_Frame = Data_Frame.withColumnRenamed("Cuenta_Next", "Cuenta_Sin_Punto")
    Data_Frame = Data_Frame.withColumnRenamed("Cuenta", "Cuenta_Real")
    Data_Frame = Data_Frame.withColumn("Recurso", lit("Mensajeria"))
    Data_Frame = Data_Frame.withColumnRenamed("Edad_Mora", "Marca")
    Data_Frame = Data_Frame.select("Cuenta_Sin_Punto", "Cuenta_Real", "Marca", "Recurso")
    
    Data_Frame = Data_Frame.withColumn("Cuenta_Real", regexp_replace(col("Cuenta_Real"), "-", ""))
    Data_Frame = Data_Frame.withColumn("Cuenta_Sin_Punto", regexp_replace(col("Cuenta_Sin_Punto"), "-", ""))

    count_df = Data_Frame.groupBy("Cuenta_Sin_Punto").agg(count("*").alias("Cantidad"))
    
    Data_Frame = Data_Frame.join(count_df, "Cuenta_Sin_Punto", "left")
    
    Data_Frame = Data_Frame.dropDuplicates(["Cuenta_Sin_Punto"])

    Data_Frame = Data_Frame.select(["Cuenta_Real", "Cuenta_Sin_Punto", "Marca", "Recurso", "Cantidad"])
    
    return Data_Frame
