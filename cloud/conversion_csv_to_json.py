import polars as pl
import os
import datetime
from typing import List

def read_csv_with_encoding_fallback(csv_path: str) -> pl.DataFrame:
    encodings = ['latin1', 'iso-8859-1', 'cp1252', 'windows-1252', 'utf-8', 'utf-8-sig']
    
    for encoding in encodings:
        try:
            df = pl.read_csv(
                csv_path,
                separator=';',
                encoding=encoding,
                infer_schema_length=0,
                ignore_errors=True
            )
            if df.height > 0:
                print(f"✔️ Success with encoding: {encoding}")
                return df
        except Exception:
            continue
    
    try:
        df = pl.read_csv(
            csv_path,
            separator=';',
            encoding='latin1',
            infer_schema_length=0,
            ignore_errors=True
        )
        print(f"⚠️ Loaded with errors using latin1 fallback")
        return df
    except Exception as e:
        raise Exception(f"All encoding attempts failed: {e}")

def convert_csv_folder_to_json(input_folder: str, output_base_path: str):
    """
    Reads all CSV files delimited by ';' in the input folder.
    Creates an output folder named 'JSON_[date] [input_folder_name]' 
    inside output_base_path.
    Applies encoding and data type corrections.
    """

    folder_name = os.path.basename(os.path.normpath(input_folder))
    timedate = datetime.datetime.now().strftime("%Y%m%d")
    final_output_folder = os.path.join(output_base_path, f"JSON_{timedate} {folder_name}")
    
    os.makedirs(final_output_folder, exist_ok=True)
    print(f"📁 Output folder created: {final_output_folder}")
    
    csv_files = [
        f for f in os.listdir(input_folder) 
        if f.lower().endswith('.csv')
    ]
    
    if not csv_files:
        print(f"⚠️ No CSV files found in: {input_folder}")
        return

    print(f"📦 Found {len(csv_files)} CSV files. Starting conversion...")

    for filename in csv_files:
        csv_path = os.path.join(input_folder, filename)
        json_filename = filename.replace('.csv', '.json')
        json_path = os.path.join(final_output_folder, json_filename)
        
        try:
            df = read_csv_with_encoding_fallback(csv_path)
            df.write_ndjson(json_path)
            print(f"✅ Converted: {filename} -> {json_filename}")

        except Exception as e:
            print(f"❌ CRITICAL ERROR processing {filename}: {e}")
            
    print("✨ Conversion process finished.")