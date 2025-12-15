import os
import polars as pl
from polars import col, lit
from typing import TYPE_CHECKING
from PyQt6.QtWidgets import QMessageBox
from skills import list_city_mins
from functools import reduce
import string
from datetime import datetime
from web.save_files import save_to_0csv, save_to_csv

### Proceso con todas las funciones desarrolladas
def Function_Complete(path, output_directory, partitions):

    Data_Frame = First_Changes_DataFrame(path)

    Data_Email = Email_Data(Data_Frame)
    Type = "Emails"
    Data_Email = Demographic_Proccess_Emails(Data_Email, output_directory, partitions)
    Save_Data_Frame(Data_Email, output_directory, partitions, Type)

    Data_Frame = Phone_Data(Data_Frame)
    Type = "Mins"
    Data_NO = Demographic_Proccess_Mins(Data_Frame, output_directory, partitions, "NO_valido")
    Data_AC = Demographic_Proccess_Mins(Data_Frame, output_directory, partitions, "valido")

    Data_Frame = pl.concat([Data_AC, Data_NO])

    Save_Data_Frame(Data_Frame, output_directory, partitions, Type)

### Cambios Generales
def First_Changes_DataFrame(Root_Path: str) -> pl.DataFrame:
    """
    Polars equivalent function to read a CSV, cast all columns to string, 
    and derive the 'Marca' column based on complex conditional logic.
    """
    
    # 1. Read the CSV file into a Polars DataFrame (eager read)
    # The original Spark code reads and immediately casts ALL columns to StringType.
    # We replicate this by reading the data and then casting all columns to Utf8 (String).
    DF = pl.read_csv(
        Root_Path, 
        has_header=True, 
        separator=";",
        encoding="latin1",
        # NEW FIX: Override schema to read all columns as Utf8 to prevent type inference errors
        # on columns that contain mixed data (like numbers and emails/text)
        schema_overrides={
            f"{i}_": pl.Utf8 for i in range(1, 60)
        } 
    )

    # 2. Cast all existing columns to Utf8 (Polars string type)
    DF = DF.with_columns(pl.all().cast(pl.Utf8))

    # --- Conditional Expressions (PySpark's 'col' becomes Polars 'pl.col') ---
    
    # Define the individual conditions as Polars boolean expressions
    potencial = (col("5_") == lit("Y")) & (col("3_") == lit("BSCS"))
    churn = (col("5_") == lit("Y")) & (col("3_") == lit("RR"))
    provision = (col("5_") == lit("Y")) & (col("3_") == lit("ASCARD"))
    prepotencial = (col("6_") == lit("Y")) & (col("3_") == lit("BSCS"))
    
    # Note: prepotencial_especial condition is stricter than prepotencial, 
    # but the original logic orders it later, so Polars follows the same order.
    prepotencial_especial = (col("6_") == lit("Y")) & (col("3_") == lit("BSCS")) & (col("12_") == lit("PrePotencial Convergente Masivo_2"))
    
    prechurn = (col("6_") == lit("Y")) & (col("3_") == lit("RR"))
    preprovision = (col("6_") == lit("Y")) & (col("3_") == lit("ASCARD"))
    castigo = col("7_") == lit("Y")
    potencial_a_castigar = (col("5_") == lit("N")) & (col("6_") == lit("N")) & (col("7_") == lit("N")) & (col("42_") == lit("Y"))
    
    # The 'otherwise' value, which is the content of column "13_"
    marcas = col("13_")

    # 3. Create the new 'Marca' column using the Polars 'when().then().otherwise()' chain
    DF = DF.with_columns(
        # Polars chain: pl.when(condition).then(value)
        pl.when(potencial).then(lit("Potencial"))
        .when(churn).then(lit("Churn"))
        .when(provision).then(lit("Provision"))
        .when(prepotencial).then(lit("Prepotencial"))
        # Note: The 'prepotencial' condition above might capture rows intended for 
        # 'prepotencial_especial' if it was placed after. Since the original Spark 
        # code placed it here, Polars maintains the execution order. 
        .when(prepotencial_especial).then(lit("Prepotencial Especial"))
        .when(prechurn).then(lit("Prechurn"))
        .when(preprovision).then(lit("Preprovision"))
        .when(castigo).then(lit("Castigo"))
        .when(potencial_a_castigar).then(lit("Potencial a Castigar"))
        .otherwise(marcas) # If none of the conditions match, use the value from col("13_")
        .alias("Marca")
    )

    return DF

### Renombramiento de columnas
def Renamed_Column(Data_Frame: pl.DataFrame) -> pl.DataFrame:
    """
    Polars equivalent function to rename columns '1_' and '2_'.
    It replicates the behavior of PySpark's withColumnRenamed.
    """
    # Use the .rename() method, which accepts a dictionary mapping 
    # old column names to new column names.
    Data_Frame = Data_Frame.rename({
        "1_": "identificacion",
        "2_": "cuenta",
    })
    
    return Data_Frame

### Proceso de guardado del RDD
def Save_Data_Frame(Data_Frame: pl.DataFrame, Directory_to_Save: str, partitions: int, Type: str) -> pl.DataFrame:
    """
    Polars equivalent function to save the DataFrame (DF) and a filtered version 
    (DF excluding 'Castigo') to CSV files, and then returns the filtered DF.
    """
    # Directory logic (requires os import)
    Type_File = "---- Bases para CARGUE ----"
    Directory_to_Save = os.path.join(Directory_to_Save, Type_File)

    delimiter = ";"
    
    # 1. Save the original DataFrame
    Name_File_Original = f"Demograficos {Type}"
    save_to_csv(Data_Frame, Directory_to_Save, Name_File_Original, partitions, delimiter)
    
    # 2. Filter the DataFrame to exclude "Castigo" (equivalent to Spark filter)
    # The logic explicitly uses "Castigo" as a string literal.
    Data_Frame_Filtered = Data_Frame.filter(col("Marca") != lit("Castigo"))
    
    # 3. Save the filtered DataFrame
    Name_File_Filtered = f"Demograficos SIN CASTIGO {Type}"
    save_to_csv(Data_Frame_Filtered, Directory_to_Save, Name_File_Filtered, partitions, delimiter)

    # 4. Return the filtered DataFrame, replicating the original function's output
    return Data_Frame_Filtered

### Dinamización de columnas de contacto
def Phone_Data(Data_: pl.DataFrame) -> pl.DataFrame:
    """
    Polars equivalent function for data preparation, focusing on cleaning and 
    stacking (melting) phone and contact data columns.
    
    FIX CRÍTICO: Se añadió 'id_vars' a la operación melt. Esto asegura que las 
    columnas de identificación ('1_', '2_', '22_', 'Marca') no se pierdan 
    durante el apilamiento de los datos de teléfono.
    """

    # --- Step 1: Replace specific substrings in '28_' column ---
    list_replace = ["VDK", "VD"]
    
    for letters in list_replace:
        Data_ = Data_.with_columns(
            pl.col("28_")
            .str.replace_all(pl.lit(letters), pl.lit("9999999999"))
            .alias("28_")
        )
        
    # --- Step 2: Define columns for stacking (melting) ---
    # Columnas para valores mínimos
    columns_to_stack_min = ["28_"] 
    # Columnas para móvil/email
    columns_to_stack_mobile = ["46_", "47_", "48_", "49_", "50_"] 
    # Columnas para líneas activas
    columns_to_stack_activelines = ["51_", "52_", "53_", "54_", "55_"] 

    all_columns_to_stack = columns_to_stack_mobile + columns_to_stack_activelines + columns_to_stack_min
    
    # Columnas a mantener como identificadores durante el melt
    id_columns_to_keep = ["1_", "2_", "22_", "Marca"] # <-- CRITICAL FIX

    # --- Step 3: Stack (Melt) the defined columns ---
    # PySpark's `stack` operation is equivalent to Polars' `melt`.
    Data_ = Data_.melt(
        id_vars=id_columns_to_keep, # Se mantienen las columnas de identificación
        value_vars=all_columns_to_stack,
        value_name="dato" 
    ).drop("variable")

    return Data_

def Email_Data(Data_: pl.DataFrame) -> pl.DataFrame:
    """
    Polars equivalent function for stacking (melting) email and phone columns 
    ("46_" to "50_").
    It replicates the PySpark stack operation.
    """

    # Columns to stack (melt)
    columns_to_stack = ["46_", "47_", "48_", "49_", "50_"] 
    
    # Columns to keep
    id_columns_to_keep = ["1_", "2_", "22_", "Marca"]
    
    all_columns_to_stack = columns_to_stack
    
    # PySpark's stack and drop logic is replaced by Polars' melt().
    # melt() creates 'variable' (original column name) and 'value' (data).
    Data_ = Data_.melt(
        id_vars=id_columns_to_keep, # <-- ADD id_vars to keep other columns
        value_vars=all_columns_to_stack,
        value_name="dato"
    ).drop("variable")

    return Data_

def Remove_Dots(dataframe: pl.DataFrame, column: str) -> pl.DataFrame:
    """
    Polars equivalent function to remove dots and hyphens from a specified column 
    (e.g., in a document number or account code) using a regular expression replacement.
    Replicates PySpark's withColumn(column, regexp_replace(col(column), "[.-]", "")).
    """
    # Use pl.with_columns and str.replace_all with a regex pattern [.-] 
    # (matching a literal dot or a hyphen) to replace them with an empty string.
    dataframe = dataframe.with_columns(
        pl.col(column)
        .str.replace_all(r"[.\-]", pl.lit(""), literal=False) # literal=False enables regex
        .alias(column)
    )
    
    return dataframe

def Demographic_Proccess_Mins(Data_: pl.DataFrame, Directory_to_Save: str, partitions: int, TypeProccess: str) -> pl.DataFrame:
    """
    Polars equivalent function for processing demographic data, specifically 
    focusing on cleaning identifier and phone columns, applying filters, 
    creating a cross-join key, and preparing the final selection.
    
    Esta función ahora recibe un Data_Frame correctamente preparado por Phone_Data.
    """

    # 1. Add static columns: ciudad, depto, tipodato
    Data_ = Data_.with_columns([
        pl.lit("BOGOTA").alias("ciudad"),
        pl.lit("BOGOTA").alias("depto"),
        pl.lit("telefono").alias("tipodato")
    ])
    
    # 2. Select initial required columns (Ahora '1_', '2_', '22_' existen)
    Data_ = Data_.select(pl.col(["1_", "2_", "22_", "ciudad", "depto", "dato", "tipodato", "Marca"]))
    
    # 3. Complex String Cleaning (fixed strings and regex)
    # Se ejecuta *antes* del cast final para evitar InvalidOperationError
    
    # 3a. Initial cleaning of "1_": remove all non-numeric characters
    Data_ = Data_.with_columns(
        pl.col("1_").str.replace_all(r"[^0-9]", pl.lit(""), literal=False).alias("1_")
    )
    
    # 3b. Remove fixed strings
    fixed_strings_to_remove = ["57- ", "57-", "57 - "]
    
    # Replace fixed strings first (literal replacement)
    for s in fixed_strings_to_remove:
        Data_ = Data_.with_columns([
            pl.col(c).str.replace_all(s, pl.lit("")).alias(c)
            for c in ["1_", "2_", "dato"]
        ])

    # 3c. Final Regex Cleaning (uppercase and removal of [A-Z\*\-\s])
    regex_to_remove = r"[A-Z\*\-\s]" 
    
    Data_ = Data_.with_columns([
        # Apply uppercase and regex cleanup to "1_" (Still String/Utf8)
        pl.col("1_").cast(pl.Utf8).str.to_uppercase().str.replace_all(regex_to_remove, pl.lit(""), literal=False).alias("1_"),
        # Apply regex cleanup to "2_" and "dato"
        pl.col("2_").str.replace_all(regex_to_remove, pl.lit(""), literal=False).alias("2_"),
        pl.col("dato").str.replace_all(regex_to_remove, pl.lit(""), literal=False).alias("dato"),
    ])

    # 4. Final Cast "1_" to Int64 (Ahora que la limpieza de strings ha terminado)
    # Filter rows where '1_' is not null after attempting to cast to Int
    Data_ = Data_.filter(
        pl.col("1_").cast(pl.Int64, strict=False).is_not_null()
    ).with_columns(
        # Cast to Int64 (similar to Spark's 'int' type conversion for ID)
        pl.col("1_").cast(pl.Int64).alias("1_")
    )
    
    # 5. Apply external filter function
    Data_ = Function_Filter(Data_, TypeProccess)
    
    # 6. Create the 'cruice' column (concatenation)
    Data_ = Data_.with_columns(
        pl.concat_str([pl.col("2_"), pl.col("dato")]).alias("cruice")
    )
    
    # 7. Drop duplicates based on 'cruice'
    Data_ = Data_.unique(subset=["cruice"], keep='first')
    
    # 8. Call Remove_Dots function
    Data_ = Remove_Dots(Data_, "2_")

    # 9. Call Renamed_Column function
    Data_ = Renamed_Column(Data_)

    # 10. Final column selection
    Data_ = Data_.select(pl.col(["identificacion", "cuenta", "ciudad", "depto", "dato", "tipodato", "Marca"]))

    return Data_

def Function_Filter(Data_: pl.DataFrame, TypeProccess: str) -> pl.DataFrame:
    """
    Polars equivalent of the original Function_Filter, applying specific numeric 
    range filters to the 'dato' column if TypeProccess is 'valido'.
    """

    if TypeProccess == "valido":
        # 1. Ensure 'dato' is numeric (Int64) for comparison. 
        # We use .cast(strict=False) because data might contain non-numeric entries 
        # (which become nulls), matching the flexible nature of Spark/Polars filtering.
        dato_int = pl.col("dato").cast(pl.Int64, strict=False)

        # 2. Define the two valid ranges using Polars' is_between and combine them with OR (|)
        valid_range_c = dato_int.is_between(3000000001, 3599999998)
        valid_range_f = dato_int.is_between(6010000001, 6089999998)
        
        # 3. Apply the combined filter to the DataFrame
        Data_ = Data_.filter(valid_range_c | valid_range_f)
    
    else:
        # The original code calls an external function: list_city_mins.lines_inactives_df(Data_)
        # Since 'list_city_mins' is not defined, we comment out the call and return the 
        # original DataFrame. You must provide this external dependency if needed.
        Data_ = list_city_mins.lines_inactives_df(Data_) 

    return Data_

def Demographic_Proccess_Emails(Data_: pl.DataFrame, Directory_to_Save: str, partitions: int) -> pl.DataFrame:
    """
    Polars equivalent function for processing demographic data, specialized for 
    email data. It cleans identifier and account columns, applies an email-specific 
    filter, creates a cross-join key, and prepares the final selection.
    
    It is derived from Demographic_Proccess_Mins but tailored for email (tipodato='email')
    and uses a simpler cleaning regex.
    """

    # 1. Add static columns: ciudad, depto, tipodato
    Data_ = Data_.with_columns([
        pl.lit("BOGOTA").alias("ciudad"),
        pl.lit("BOGOTA").alias("depto"),
        pl.lit("email").alias("tipodato") # Set to 'email'
    ])
    
    # 2. Select initial required columns
    Data_ = Data_.select(pl.col(["1_", "2_", "22_", "ciudad", "depto", "dato", "tipodato", "Marca"]))

    # 3. Clean and cast column "1_" (identificacion)
    Data_ = Data_.with_columns(
        # Remove all non-numeric characters
        pl.col("1_").str.replace_all(r"[^0-9]", pl.lit(""), literal=False).alias("1_")
    )
    # Filter rows where '1_' is not null after attempting to cast to Int
    Data_ = Data_.filter(
        pl.col("1_").cast(pl.Int64, strict=False).is_not_null()
    ).with_columns(
        # Cast to Int64 (similar to Spark's 'int' type conversion for ID)
        pl.col("1_").cast(pl.Int64).alias("1_")
    )
    
    # 4. Simple String Cleaning for '1_' and '2_'
    # Regex to remove: [A-Z] (uppercase letters) and [\*]
    regex_to_remove = r"[A-Z\*]" 
    
    Data_ = Data_.with_columns([
        # Apply uppercase and regex cleanup to "1_"
        pl.col("1_").cast(pl.Utf8).str.to_uppercase().str.replace_all(regex_to_remove, pl.lit(""), literal=False).alias("1_"),
        # Apply regex cleanup to "2_"
        pl.col("2_").str.replace_all(regex_to_remove, pl.lit(""), literal=False).alias("2_"),
        # Note: 'dato' cleaning (for email content) is skipped, mirroring the PySpark logic.
    ])
    
    # 5. Apply email-specific filter function (Placeholder)
    Data_ = Function_Filter_Email(Data_)
    
    # 6. Create the 'cruice' column (concatenation)
    Data_ = Data_.with_columns(
        pl.concat_str([pl.col("2_"), pl.col("dato")]).alias("cruice")
    )
    
    # 7. Drop duplicates based on 'cruice'
    Data_ = Data_.unique(subset=["cruice"], keep='first')
    
    # 8. Call Remove_Dots function
    Data_ = Remove_Dots(Data_, "2_")

    # 9. Call Renamed_Column function
    Data_ = Renamed_Column(Data_)

    # 10. Final column selection
    Data_ = Data_.select(pl.col(["identificacion", "cuenta", "ciudad", "depto", "dato", "tipodato", "Marca"]))

    return Data_

def Function_Filter_Email(Data_: pl.DataFrame) -> pl.DataFrame:
    """
    Polars equivalent of the PySpark function to clean and categorize email data 
    in the 'dato' column by applying length checks, '@' counts, and checking for 
    a list of problematic keywords.
    """
    
    # Helper variable for split operation
    dato_split = pl.col("dato").str.split("@")

    # 1. Initial Tipologia calculation based on email structure
    Data_ = Data_.with_columns(
        # FIX: Replaced .str.lengths() with the correct Polars method, .str.len_chars()
        pl.when(dato_split.list.get(0).str.len_chars() < lit(6))  # Part before '@' length < 6
        .then(lit("ERRADO"))
        .when(dato_split.list.len() == lit(2)) # Exactly one '@'
        .then(lit("CORREO UNICO"))
        .when(dato_split.list.len() >= lit(3)) # Two or more '@'
        .then(lit("CORREOS SIN DELIMITAR"))
        .otherwise(lit("ERRADO"))
        .alias("Tipologia")
    )

    # 2. Filter: Must contain '@' (literal=True for simple substring search)
    Data_ = Data_.filter(pl.col("dato").str.contains("@", literal=True))
    
    # 3. Define problematic strings
    list_email_replace = [
        "notiene", "nousa", "nobrinda", "000@00.com.co", "nolorecuerda", "notengo", "noposee",
        "nosirve", "notien", "noutili", "nomanej", "nolegust", "nohay", "nocorreo", "noindic",
        "nohay", "@gamil", "pendienteconfirmar", "sincorr", "pendienteporcrearclaro", "correo.claro",
        "crearclaro", ":", "|", " ", "porcrear", "+", "#", "@xxx", "-", "@claro", "suministra", 
        "factelectronica", "nodispone"
    ]

    email_set = set(list_email_replace)

    # 4. Lowercase 'dato'
    Data_ = Data_.with_columns(
        pl.col("dato").str.to_lowercase().alias("dato")
    )

    # 5. Build the OR expression to check if 'dato' contains any problematic word
    # This replicates the Spark 'reduce' pattern using Polars expressions.
    contains_any_expr = reduce(
        lambda acc, word: acc | pl.col("dato").str.contains(word, literal=True),
        email_set,
        pl.lit(False)
    )

    # 6. Update Tipologia: Mark as ERRADO if any problematic word is found
    Data_ = Data_.with_columns(
        pl.when(contains_any_expr)
        .then(pl.lit("ERRADO"))
        .otherwise(pl.col("Tipologia"))
        .alias("Tipologia")
    )
    
    # 7. Final filters
    Data_ = Data_.filter(pl.col("dato") != pl.lit("@"))
    Data_ = Data_.filter(pl.col("Tipologia") != pl.lit("ERRADO"))

    return Data_