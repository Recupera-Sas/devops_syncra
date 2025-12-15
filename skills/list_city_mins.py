import polars as pl

def lines_inactives_df(data_frame: pl.DataFrame) -> pl.DataFrame:
    """
    Polars equivalent of PySpark function to process and standardize inactive 
    phone lines (MINS) based on location and length.

    This function isolates data outside the initial valid MINS range, attempts 
    to determine the area code (INDICATIVO) from the 'LUGAR' column, 
    and reconstructs the full phone number (dato).
    """
    
    # 0. Convert 'dato' to numeric for range checks (casting to Int64 is necessary 
    # since all columns were cast to Utf8 earlier in First_Changes_DataFrame).
    dato_int = data_frame.get_column("dato").cast(pl.Int64, strict=False)

    data_frame = data_frame.rename({"22_": "LUGAR"})

    # Filter 1: dato <= 3000000000
    Data_1 = data_frame.filter(dato_int <= pl.lit(3000000000))
    
    # Filter 2: dato >= 3599999999 AND dato <= 6010000000
    Data_2 = data_frame.filter(
        (dato_int >= pl.lit(3599999999)) & (dato_int <= pl.lit(6010000000))
    )

    # Union/Vertical stacking of filtered dataframes (equivalent to PySpark union)
    data_frame = pl.concat([Data_1, Data_2])

    # Standardize 'LUGAR' column: replace empty/null with "BOGOTA"
    data_frame = data_frame.with_columns(
        pl.when((pl.col("LUGAR") == pl.lit("")) | (pl.col("LUGAR").is_null()))
        .then(pl.lit("BOGOTA"))
        .otherwise(pl.col("LUGAR"))
        .alias("LUGAR")
    )

    # Get the first element after splitting 'LUGAR' by "/"
    data_frame = data_frame.with_columns(
        pl.col("LUGAR").str.split("/").list.get(0).alias("LUGAR")
    )
    
    # Consolidate 'replace_all' calls for spaces
    # Remove all whitespace characters from 'dato'
    data_frame = data_frame.with_columns(
        pl.col("dato").str.replace_all(r"\s+", pl.lit(""), literal=False).alias("dato"),
    )
    
    # Calculate length of 'dato' and store original value in 'TELEFONO'
    data_frame = data_frame.with_columns([
        # CORRECTED: Use pl.col("dato").str.len_chars() for string length in Polars
        pl.col("dato").str.len_chars().alias("LARGO"),
        pl.col("dato").alias("TELEFONO"),
        pl.lit("").alias("dato"), # Reset 'dato' column to empty string
    ])

    # Define lists for regional indicative matching
    list1 = ["BOGOTA", "CUNDINAMARCA", "SOACHA", "BOGOTÁ", "BOGOT"]
    list2 = ["CAUCA", "NARIÑO", "VALLE", "CALI", "JAMUNDI", "JAMUNDÍ"]
    list3 = ["ANTIOQUIA", "BARRANQUILLA", "CORDOBA", "CHOCO", "MEDELLÍN", "MEDELLIN", "MEDELL"]
    list4 = ["ATLANTICO", "BOLIVAR", "CESAR", "LA GUAJIRA", "MAGDALENA", "SUCRE"]
    list5 = ["CALDAS", "QUINDIO", "RISARALDA"]
    list6 = ["ARAUCA", "NORTE DE SANTANDER", "SANTANDER"]
    list7 = ["AMAZONAS", "BOYACA", "CASANARE", "CAQUETA", "GUAVIARE", "GUAINIA", "HUILA", "META", "TOLIMA", "PUTUMAYO", "SAN ANDRES", "VAUPES", "VICHADA"]

    # Final cleanup of 'LUGAR' (remove characters that are not uppercase letters or spaces)
    data_frame = data_frame.with_columns(
        pl.col("LUGAR").str.replace_all(r"[^A-Z ]", pl.lit(""), literal=False).alias("LUGAR")
    )
    
    # Get the first word of 'LUGAR' after cleaning and splitting by space
    data_frame = data_frame.with_columns(
        pl.col("LUGAR").str.split(" ").list.get(0).alias("LUGAR")
    )

    # Determine INDICATIVO (area code) based on location lists
    data_frame = data_frame.with_columns(
        pl.when(pl.col("LUGAR").str.to_uppercase().is_in(list1)).then(pl.lit("1"))
        .when(pl.col("LUGAR").str.to_uppercase().is_in(list2)).then(pl.lit("2"))
        .when(pl.col("LUGAR").str.to_uppercase().is_in(list3)).then(pl.lit("4"))
        .when(pl.col("LUGAR").str.to_uppercase().is_in(list4)).then(pl.lit("5"))
        .when(pl.col("LUGAR").str.to_uppercase().is_in(list5)).then(pl.lit("6"))
        .when(pl.col("LUGAR").str.to_uppercase().is_in(list6)).then(pl.lit("7"))
        .when(pl.col("LUGAR").str.to_uppercase().is_in(list7)).then(pl.lit("8"))
        .otherwise(pl.lit("000"))
        .alias("INDICATIVO")
    )

    # Filter out rows where INDICATIVO could not be determined
    data_frame = data_frame.filter(pl.col("INDICATIVO") != pl.lit("000"))

    # Reconstruct 'dato' (the phone number) based on length
    data_frame = data_frame.with_columns(
        pl.when(pl.col("LARGO") == pl.lit(7)).then(pl.concat_str([pl.lit("60"), pl.col("INDICATIVO"), pl.col("TELEFONO")]))
        .when(pl.col("LARGO") == pl.lit(8)).then(pl.concat_str([pl.lit("60"), pl.col("TELEFONO")]))
        .when(pl.col("LARGO") == pl.lit(9)).then(pl.concat_str([pl.lit("6"), pl.col("TELEFONO")]))
        .otherwise(pl.lit(""))
        .alias("dato")
    )

    # Final filter by the length of the original 'dato' (before reconstruction)
    data_frame = data_frame.filter(
        (pl.col("LARGO") >= pl.lit(7)) & (pl.col("LARGO") <= pl.lit(8))
    )
    
    # Re-cast 'dato' to numeric for the final range checks
    dato_int_final = data_frame.get_column("dato").cast(pl.Int64, strict=False)
    
    # Final filter by valid phone number ranges (post-reconstruction)
    Data_C = data_frame.filter(
        (dato_int_final >= pl.lit(3000000001)) & (dato_int_final <= pl.lit(3599999998))
    )
    Data_F = data_frame.filter(
        (dato_int_final >= pl.lit(6010000000)) & (dato_int_final <= pl.lit(6089999998))
    )
    
    # Final union of valid phone ranges
    data_frame = pl.concat([Data_C, Data_F])

    # Final selection of required columns
    data_frame = data_frame.select(pl.col(["1_", "2_", "ciudad", "depto", "dato", "tipodato", "Marca"]))

    return data_frame