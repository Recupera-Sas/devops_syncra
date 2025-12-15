import os
from web.pyspark_session import get_spark_session
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, length
from web.save_files import save_to_csv

spark = get_spark_session()
 
def read_files_insignias(file_path, output_directory):

    # Initialize an empty DataFrame for consolidation
    consolidated_df = None

    union_df = None
    
    # Iterate through all files in the directory
    for root, _, files in os.walk(file_path):
        for file in files:
            if file.endswith('.csv'):
                file_full_path = os.path.join(root, file)
                print(f"Processing file: {file_full_path}")

                # Read the current CSV file
                df = spark.read.option("header", "true").option("delimiter", ",").csv(file_full_path)

                # Initialize the selected columns
                selected_columns = ['Día', 'Tipologia Grupo', 'Agente', 'Productividad', 'Efectividad', 'Tipologia']

                # Check for the presence of the additional columns
                if 'Acuerdos' in df.columns:
                    df = df.withColumnRenamed('Acuerdos', 'Productividad')
                    df = df.withColumn('Tipologia', lit('Recaudo')) 
                    
                if 'Pagos' in df.columns:
                    df = df.withColumnRenamed('Pagos', 'Efectividad')
                    df = df.withColumn('Tipologia', lit('Recaudo')) 
                    
                if 'Servicios Gestionados' in df.columns:
                    df = df.withColumnRenamed('Servicios Gestionados', 'Productividad')
                    df = df.withColumn('Tipologia', lit('Cuentas')) 
                    
                if 'Servicios Recuperados' in df.columns:
                    df = df.withColumnRenamed('Servicios Recuperados', 'Efectividad')
                    df = df.withColumn('Tipologia', lit('Cuentas')) 

                # Select the relevant columns
                filtered_df = df.select([col for col in selected_columns if col in df.columns])

                # Rename the column 'Día' to 'Dia Acuerdo'
                df = df.withColumnRenamed('Día', 'Dia Acuerdo')
                df = df.filter('Productividad', 'Dia Acuerdo')
                
                # Consolidate the DataFrame
                if consolidated_df is None:
                    consolidated_df = filtered_df
                else:
                    consolidated_df = consolidated_df.unionByName(filtered_df, allowMissingColumns=True)

            if union_df is None:
                print("union_df is None")
                union_df = consolidated_df
                
            else:
                print("union_df is not None")
                union_df = union_df.union(consolidated_df)

    # Save the result to CSV with a specific delimiter
    delimiter = ';'
    filename = "Insignias"
    Partitions = 1

    save_to_csv(union_df, output_directory, filename, Partitions, delimiter)

    return consolidated_df