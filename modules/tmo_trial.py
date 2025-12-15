import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession, SQLContext 
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, lit
from web.pyspark_session import get_spark_session
from web.save_files import save_to_csv
import pandas as pd

def Function_Complete(Path, Outpath, Partitions):

    spark = get_spark_session()

    sqlContext = SQLContext(spark)
    now = datetime.now()

    temp_path = Path.replace('.csv', '_clean.csv')
    df_pd = pd.read_csv(Path, skiprows=3)
    df_pd.to_csv(temp_path, index=False)

    df = spark.read.csv(temp_path, header=True, sep=",")
    df = df.select([col(c).cast(StringType()).alias(c) for c in df.columns])

    dfcolumns = ["Usuario","identificación","LLAMADAS","HORA","tiempo de inicio de sesión",
                 "ESPERA","ESPERE%","CHARLA","CHARLA TIEMPO%","DISPO","DISPOTIME%","Pausa",
                 "pausetime%","DEAD","TIEMPO MUERTO%","CLIENTE","CONNECTED","ALMUER","BANO",
                 "BKAM","BKPM","LAGGED","LOGIN","PAUACT","RETROA","REUNIO","VISIBLE","HIDDEN"]
    

    df_real_columns = df.columns
    for column_df in dfcolumns:
        if column_df in df_real_columns:
            pass
        else:
            df = df.withColumn(column_df, lit(""))

    dflistcolumns = df.columns
    
    if "BREAK" not in dflistcolumns:
        df = df.withColumnRenamed('BKAM', 'BREAK')
    
    if "BREAKP" not in dflistcolumns:        
        df = df.withColumnRenamed('BKPM', 'BREAKP')
    
    if "PACTIV" not in dflistcolumns:
        df = df.withColumnRenamed('PAUACT', 'PACTIV')
    
    if "REU" not in dflistcolumns:
        df = df.withColumnRenamed('REUNIO', 'REU')
    
    if "identificacion" not in dflistcolumns:
        df = df.withColumnRenamed('identificación', 'identificacion')
    
    df = df.withColumnRenamed('tiempo de inicio de sesión', 'tiempo de inicio de sesion')

    now = datetime.now()
    yesterday = now - timedelta(days=1)
    Yesterday_Date = yesterday.strftime("%d/%m/%Y")

    df = df.withColumn("Fecha", lit(f"{Yesterday_Date}"))

    Type_Proccess = f"TMO Conversion"
    delimiter = ";"
    
    save_to_csv(df, Outpath, Type_Proccess, Partitions, delimiter)

    try:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        if os.path.exists(Path):
            os.remove(Path)
    except Exception as e:
        print(f"Error eliminando archivos: {e}")
        
    return df