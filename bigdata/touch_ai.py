import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, coalesce, lower
from web.pyspark_session import get_spark_session
from web.save_files import save_to_csv

def touch_dataframes_bd(input_folder, output_path, num_partitions, month_data, year_data):
    
    spark = get_spark_session()

    # File paths
    Root_SMS = f"{input_folder}/Consolidado SMS.csv"
    Root_Email = f"{input_folder}/Consolidado EMAIL.csv"
    Root_BOT = f"{input_folder}/Consolidado BOT.csv"
    Root_IVR = f"{input_folder}/Consolidado IVR.csv"

    try:
        # Read and prepare SMS DataFrame
        File_SMS = spark.read.csv(Root_SMS, header=True, sep=";").select(
            col("Cuenta_Real"),
            col("Cuenta_Sin_Punto"),
            col("Marca"),
            col("Cantidad").alias("Toques por SMS")
        )

        # Read and prepare Email DataFrame
        File_Email = spark.read.csv(Root_Email, header=True, sep=";").select(
            col("Cuenta_Real"),
            col("Cuenta_Sin_Punto"),
            col("Marca"),
            col("Cantidad").alias("Toques por EMAIL")
        )

        # Read and prepare BOT DataFrame
        File_BOT = spark.read.csv(Root_BOT, header=True, sep=";").select(
            col("Cuenta_Real"),
            col("Cuenta_Sin_Punto"),
            col("Marca"),
            col("Cantidad").alias("Toques por BOT")
        )

        # Read and prepare IVR DataFrame
        File_IVR = spark.read.csv(Root_IVR, header=True, sep=";").select(
            col("Cuenta_Real"),
            col("Cuenta_Sin_Punto"),
            col("Marca"),
            col("Cantidad").alias("Toques por IVR")
        )

        # Perform full outer joins
        Data_Frame = File_SMS.join(File_Email, on=["Cuenta_Real", "Cuenta_Sin_Punto", "Marca"], how="full_outer")
        Data_Frame = Data_Frame.join(File_BOT, on=["Cuenta_Real", "Cuenta_Sin_Punto", "Marca"], how="full_outer")
        Data_Frame = Data_Frame.join(File_IVR, on=["Cuenta_Real", "Cuenta_Sin_Punto", "Marca"], how="full_outer")

        if Data_Frame is not None:
            # Coalesce to handle empty values
            Data_Frame = Data_Frame.withColumn("Toques por SMS", coalesce(col("Toques por SMS"), lit(0)))
            Data_Frame = Data_Frame.withColumn("Toques por EMAIL", coalesce(col("Toques por EMAIL"), lit(0)))
            Data_Frame = Data_Frame.withColumn("Toques por BOT", coalesce(col("Toques por BOT"), lit(0)))
            Data_Frame = Data_Frame.withColumn("Toques por IVR", coalesce(col("Toques por IVR"), lit(0)))

            Data_Frame = Data_Frame.withColumn("MES DE ASIGNACION", lit(month_data))
            Data_Frame = Data_Frame.withColumn("PERIODO DE ASIGNACION", lit(year_data))

            Data_Frame = Data_Frame.dropDuplicates(["Cuenta_Sin_Punto"])
            
            Wallet_Brand = ["0", "30", "potencial", "prechurn", "churn", "prepotencial",
                            "60", "90", "120", "150", "180", "210", "apple manual", "prepotencial especial",
                            "castigo", "provision", "preprovision"]
            
            Data_Frame = Data_Frame.withColumn("marca", lower(col("marca")))
            Data_Frame = Data_Frame.filter(col("marca").isin(Wallet_Brand))
            
            Type_File = f"Toques Telematica"

            delimiter = ";"
            
            save_to_csv(Data_Frame, output_path, Type_File, num_partitions, delimiter)
            
        else:
            print("No data was merged.")
        return Data_Frame
    except Exception as e:
        print(f"Error in touch_dataframes_bd: {e}")
        return None