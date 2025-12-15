from datetime import datetime
import os
from pyspark.sql.functions import regexp_replace, count, substring
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, when, expr, concat, lit, row_number, collect_list, concat_ws, trim, regexp_replace, length
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from web.pyspark_session import get_spark_session
from web.save_files import save_to_csv

spark = get_spark_session()

sqlContext = SQLContext(spark)

def clean_and_rename_columns(df):

    # Clean column names: remove spaces, accents, and special characters
    for old_col in df.columns:
        new_col = (
            old_col.lower()  # Convert to lowercase
            .replace(" ", "_")  # Replace spaces with underscores
        )
        df = df.withColumnRenamed(old_col, new_col)

    # Rename specific columns
    rename_mapping = {
        "estado": "status",
        "id_de_la_campaña": "vendor_lead_code",
        "identificacion": "source_id",
        "lead_id": "last_name",
        "numero_marcado": "phone_number",
        "creador": "title",
        "empresa": "first_name",
        "nombre_de_la_campaña": "list_id"
    }

    print(df.columns)
    
    for old_name, new_name in rename_mapping.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
        
    print(df.columns)

    return df


def function_complete_IVR(input_folder, output_folder, partitions, Widget_Process):  
    
    files = []

    # Collect all CSV and TXT files
    for root, _, file_names in os.walk(input_folder):
        for file_name in file_names:
            if file_name.endswith('.csv') or file_name.endswith('.txt'):
                files.append(os.path.join(root, file_name))

    consolidated_df = None
    for file in files:
        if file.endswith('.csv'):
            df_csv = spark.read.csv(file, header=True, sep=";", inferSchema=True)
            df = clean_and_rename_columns(df_csv)
        elif file.endswith('.txt'):
            df = spark.read.option("delimiter", "\t").csv(file, header=True, inferSchema=True)
        else:
            print("Data no validate")
        
        # Select only the required columns
        required_columns = [
            "status", "vendor_lead_code", "source_id", "list_id",
            "phone_number", "title", "first_name", "last_name"
        ]
        
        for col_df in required_columns:
            if col_df not in df.columns:
                df = df.withColumn(col_df, lit(None).cast(StringType()))
        
        df = df.select(*[col for col in required_columns if col in df.columns])

        # Consolidate DataFrames
        if consolidated_df is None:
            consolidated_df = df
        else:
            consolidated_df = consolidated_df.unionByName(df, allowMissingColumns=True)

    if consolidated_df is not None:
        print(consolidated_df.columns)
        # Continue with the existing transformations
        consolidated_df = consolidated_df.withColumn(
            "status",
            when(col("status") == "AB", "Buzon de voz lleno")
            .when(col("status") == "PM", "Se inicia mensaje y cuelga")                          # Efectivo
            .when(col("status") == "AA", "Maquina contestadora")
            .when(col("status") == "ADC", "Numero fuera de servicio")
            .when(col("status") == "DROP", "No contesta")
            .when(col("status") == "NA", "No contesta")
            .when(col("status") == "NEW", "Contacto sin marcar")
            .when(col("status") == "PDROP", "Error desde el operador")
            .when(col("status") == "PU", "Cuelga llamada")                                      
            .when(col("status") == "SVYEXT", "Llamada transferida al agente")                   # Efectivo
            .when(col("status") == "XFER", "Se reprodujo mensaje completo")                     # Efectivo
            .when(col("status") == "SVYHU", "Cuelga durante una transferencia")                 # Efectivo
            .otherwise(col("status"))
        )

        list_efecty = [
            "Buzon de voz lleno", "Se inicia mensaje y cuelga", "Maquina contestadora",
            "Cuelga llamada", "Llamada transferida al agente", "Se reprodujo mensaje completo",
            "Cuelga durante una transferencia", "EFECTIVO", "MARCADO", "NO EFECTIVO", "PARA REMARCAR",
            "ANSWERED", "BUSY", "NO ANSWER",
        ]

        consolidated_df = consolidated_df.filter(col("status").isin(list_efecty))
        
        consolidated_df = consolidated_df.withColumn(
            "Campana",
            when(col("list_id") == "4251", "GMAC")
            .when(col("list_id") == "4181", "PASH")
            .when(length(col("list_id")) > 5, col("list_id"))
            .otherwise("CLARO")
        )

        consolidated_df = consolidated_df.withColumn(
            "first_name",
            when(col("first_name").isNull() | (trim(col("first_name")) == ""), "0")
            .otherwise(col("first_name"))
        )

        consolidated_df = Function_Modify(consolidated_df)
        delimiter = ";"
        Type_Proccess = "Consolidado IVR"
        
        print(consolidated_df.count())
        save_to_csv(consolidated_df, output_folder, Type_Proccess, partitions, delimiter)
        
    else:
        print("Not saved file")
        
    return consolidated_df


def Function_Modify(RDD):
    
    Data_Frame = RDD.withColumn("source_id", regexp_replace(col("source_id"), "-", ""))

    Data_Frame = Data_Frame.withColumnRenamed("source_id", "Cuenta_Sin_Punto")
    Data_Frame = Data_Frame.withColumn("Recurso", lit("IVR"))
    Data_Frame = Data_Frame.withColumn("Cuenta_Real", col("Cuenta_Sin_Punto"))
    Data_Frame = Data_Frame.withColumnRenamed("first_name", "Marca")
    #Data_Frame = Data_Frame.select("Cuenta_Sin_Punto", "Cuenta_Real", "Marca", "Recurso")

    Data_Frame = Data_Frame.withColumn("Cuenta_Real", regexp_replace(col("Cuenta_Real"), "-", ""))
    Data_Frame = Data_Frame.withColumn("Cuenta_Sin_Punto", regexp_replace(col("Cuenta_Sin_Punto"), "-", ""))
    
    count_df = Data_Frame.groupBy("Cuenta_Sin_Punto").agg(count("*").alias("Cantidad"))
    
    Data_Frame = Data_Frame.join(count_df, "Cuenta_Sin_Punto", "left")
    #Data_Frame = Data_Frame.filter(col("Cuenta_Sin_Punto") == "822514404")
    
    Data_Frame = Data_Frame.dropDuplicates(["Cuenta_Sin_Punto"])

    Data_Frame = Data_Frame.select(["Cuenta_Real", "Cuenta_Sin_Punto", "Marca", "Recurso", "Cantidad"])
    
    return Data_Frame