import polars as pl
import os
from pathlib import Path
from datetime import datetime

def process_call_files(input_folder: str, output_folder: str):
    dataframes = []
    total_records = 0
    
    files = sorted(Path(input_folder).glob("*"))
    
    for file in files:
        if file.suffix.lower() in ['.csv', '.parquet']:
            print(f"Processing: {file.name}")
            
            try:
                if file.suffix.lower() == '.csv':
                    try:
                        df = pl.read_csv(
                            file, 
                            separator=';',
                            infer_schema_length=10000,
                            truncate_ragged_lines=True,
                            ignore_errors=True
                        )
                    except:
                        df = pl.read_csv(
                            file,
                            infer_schema_length=10000,
                            truncate_ragged_lines=True,
                            ignore_errors=True
                        )
                else:
                    df = pl.read_parquet(file)
                
                if 'fechagestion' in df.columns and 'perfil' in df.columns:
                    records = df.height
                    total_records += records
                    
                    df_clean = df.select([
                        pl.col('fechagestion').cast(pl.Utf8),
                        pl.col('perfil').cast(pl.Utf8)
                    ]).drop_nulls()
                    
                    dataframes.append(df_clean)
                    print(f"  ✅ {records:,} valid records")
                else:
                    print(f"  ⚠️  Missing required columns in {file.name}")
                    
            except Exception as e:
                print(f"  ❌ Error processing {file.name}")
    
    if not dataframes:
        print("No valid files found")
        return
    
    print(f"\nCombining {len(dataframes)} files...")
    combined_df = pl.concat(dataframes)
    print(f"Total records: {combined_df.height:,}")
    
    print("Processing dates...")
    result = process_dates_and_count(combined_df)
    
    if result is None:
        print("Failed to process dates")
        return
    
    current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_name = f"call_count_{current_date}.xlsx"
    output_path = os.path.join(output_folder, output_name)
    
    print(f"Saving to: {output_path}")
    result.write_excel(output_path)
    
    csv_name = f"call_count_{current_date}.csv"
    csv_path = os.path.join(output_folder, csv_name)
    result.write_csv(csv_path, separator=';')
    
    print(f"\n✅ Process completed!")
    print(f"Records: {combined_df.height:,}")
    print(f"Date range: {result['fecha'].min()} to {result['fecha'].max()}")
    print(f"Unique profiles: {result['perfil'].n_unique()}")
    
    return result

def process_dates_and_count(df):
    date_formats = [
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y",
    ]
    
    for date_format in date_formats:
        try:
            result = (df
                .with_columns([
                    pl.col('fechagestion')
                    .str.to_datetime(format=date_format, strict=False)
                    .dt.date()
                    .alias('fecha')
                ])
                .drop_nulls(['fecha', 'perfil'])
                .group_by(['fecha', 'perfil'])
                .agg(pl.len().alias('count'))
                .sort(['fecha', 'perfil'])
            )
            
            if result.height > 0:
                return result
        except:
            continue
    
    try:
        result = (df
            .with_columns([
                pl.col('fechagestion')
                .str.slice(0, 10)
                .alias('fecha')
            ])
            .group_by(['fecha', 'perfil'])
            .agg(pl.len().alias('count'))
            .sort(['fecha', 'perfil'])
        )
        return result
    except:
        return None