from web.save_files import save_to_csv
import os
from datetime import datetime
import polars as pl
from polars import col, lit
from typing import TYPE_CHECKING
 
def Function_Exclusions(Path: str, Outpath: str, Partitions: int) -> pl.DataFrame:
    """
    Polars equivalent of the original PySpark function.
    Reads an exclusion file, filters for 'Reclamacion' profiles, 
    cleans the account number, and prepares the final output DataFrame.
    """
    
    # Calculate the fixed date string once (eagerly)
    current_date = datetime.now().strftime("%Y-%m-%d")

    # Required columns for the operation
    management_cols = ["cuenta", "perfil_historico", "ultimo_perfil", "mejorperfil"]
    
    # 1. Start the LazyFrame scan (equivalent to spark.read.csv)
    # Scan_csv is used for performance, executing the plan lazily.
    ldf = (
        pl.scan_csv(
            Path, 
            has_header=True, 
            separator=";",
            # Assuming the file is read as UTF8 strings for the cleaning and filtering steps.
        )
        
        # 2. Select the required columns
        .select(management_cols)
        
        # 3. Clean 'cuenta' Column: Remove hyphens (equivalent to regexp_replace(col("cuenta"), "-", ""))
        .with_columns(
            pl.col("cuenta")
            .str.replace_all("-", "", literal=True) # literal=True for simple string replacement
            .alias("cuenta")
        )
        
        # 4. Select/Reorder and Deduplicate
        .select("cuenta", "ultimo_perfil", "mejorperfil", "perfil_historico")
        .unique() # dropDuplicates() equivalent
        
        # 5. Filter Logic: Filter where all three profile columns equal "Reclamacion"
        .filter(
            (pl.col("ultimo_perfil") == "Reclamacion") &
            (pl.col("mejorperfil") == "Reclamacion") &
            (pl.col("perfil_historico") == "Reclamacion")
        )
        
        # 6. Add Date Column and Rename
        .with_columns(
            pl.lit(current_date).alias("FECHA") # Add fixed date string column (equivalent to lit(datetime.now()...))
        )
        .rename({"cuenta": "CUENTA"}) # withColumnRenamed() equivalent
        
        # 7. Final Selection
        .select("CUENTA", "FECHA")
    )
    
    # 8. Collect (execute the lazy plan and return an eager DataFrame)
    df = ldf.collect()

    # 9. Save and Return (Assumes Save_File_Form is adapted for Polars DF)
    Save_File_Form(df, Outpath, Partitions)
    
    return df

def Save_File_Form(df, Outpath, Partitions):

    Type_File = "No Gestion Perfiles"
    delimiter = ";"

    Outpath = f"{Outpath}---- Bases para CARGUE ----"
    save_to_csv(df, Outpath, Type_File, Partitions, delimiter)

    return df