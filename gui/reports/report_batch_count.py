import pandas as pd
import openpyxl
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, PatternFill, Border, Side
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
            print(f"Procesando: {file.name}")
            
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
                
                required_cols = ['fechagestion', 'perfil', 'cuenta_promesa']
                if all(col in df.columns for col in required_cols):
                    records = df.height
                    total_records += records
                    
                    df_clean = df.select([
                        pl.col('fechagestion').cast(pl.Utf8),
                        pl.col('perfil').cast(pl.Utf8),
                        pl.col('cuenta_promesa').cast(pl.Utf8)
                    ]).drop_nulls()
                    
                    dataframes.append(df_clean)
                    print(f"  ✅ {records:,} registros válidos")
                else:
                    missing = [col for col in required_cols if col not in df.columns]
                    print(f"  ⚠️  Columnas faltantes en {file.name}: {missing}")
                    
            except Exception as e:
                print(f"  ❌ Error procesando {file.name}")
    
    if not dataframes:
        print("No se encontraron archivos válidos")
        return
    
    print(f"\nCombinando {len(dataframes)} archivos...")
    combined_df = pl.concat(dataframes)
    print(f"Total registros: {combined_df.height:,}")
    
    print("Procesando fechas...")
    df_with_dates = add_date_column(combined_df)
    
    if df_with_dates is None:
        print("Error al procesar fechas")
        return
    
    df_with_dates = df_with_dates.with_columns([
        pl.when(pl.col('perfil').str.contains("BLASTER CONTROLNEXT|IVR SAEM"))
        .then(pl.lit("IVR"))
        .when(pl.col('perfil').str.contains("MENSAJERIA"))
        .then(pl.lit("SMS"))
        .when(pl.col('perfil').str.contains("CORREO"))
        .then(pl.lit("EMAIL"))
        .otherwise(pl.col('perfil'))
        .alias('herramienta')
    ])
    
    print("Generando detalle general...")
    detalle_general = (df_with_dates
        .group_by(['fecha', 'perfil', 'herramienta'])
        .agg(pl.len().alias('contador'))
        .sort(['fecha', 'herramienta', 'perfil'])
    )
    
    print("Generando detalle sin duplicados...")
    detalle_unicos = (df_with_dates
        .unique(subset=['cuenta_promesa', 'fecha', 'herramienta'])
        .group_by(['fecha', 'perfil', 'herramienta'])
        .agg(pl.len().alias('contador'))
        .sort(['fecha', 'herramienta', 'perfil'])
    )
    
    print("Generando resumen general (tabla dinámica)...")
    resumen_general = (detalle_general
        .group_by(['fecha', 'herramienta'])
        .agg(pl.sum('contador').alias('total'))
        .pivot(
            on='herramienta',
            index='fecha',
            values='total',
            aggregate_function='first'
        )
        .fill_null(0)
        .sort('fecha')
    )
    
    print("Generando resumen sin duplicados (tabla dinámica)...")
    resumen_unicos = (detalle_unicos
        .group_by(['fecha', 'herramienta'])
        .agg(pl.sum('contador').alias('total'))
        .pivot(
            on='herramienta',
            index='fecha',
            values='total',
            aggregate_function='first'
        )
        .fill_null(0)
        .sort('fecha')
    )
    
    current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(output_folder, f"reporte_recuentos_batch_{current_date}.xlsx")
    
    print(f"Guardando: {output_path}")
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        resumen_general.to_pandas().to_excel(writer, sheet_name='Resumen General', index=False)
        resumen_unicos.to_pandas().to_excel(writer, sheet_name='Resumen Unicos', index=False)
        detalle_general.to_pandas().to_excel(writer, sheet_name='Detalle General', index=False)
        detalle_unicos.to_pandas().to_excel(writer, sheet_name='Detalle Unicos', index=False)
        
        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            for row in worksheet.iter_rows():
                for cell in row:
                    if cell.row == 1:
                        cell.font = openpyxl.styles.Font(bold=True)
                        cell.fill = openpyxl.styles.PatternFill(start_color="D3D3D3", end_color="D3D3D3", fill_type="solid")
                    cell.border = openpyxl.styles.Border(
                        left=openpyxl.styles.Side(style='thin'),
                        right=openpyxl.styles.Side(style='thin'),
                        top=openpyxl.styles.Side(style='thin'),
                        bottom=openpyxl.styles.Side(style='thin')
                    )
            
            for column in worksheet.columns:
                max_length = 0
                column_letter = openpyxl.utils.get_column_letter(column[0].column)
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
    
    print(f"\n✅ Proceso completado!")
    print(f"Total registros procesados: {combined_df.height:,}")
    print(f"Registros únicos: {df_with_dates.unique(subset=['cuenta_promesa', 'fecha', 'herramienta']).height:,}")
    print(f"Rango de fechas: {df_with_dates['fecha'].min()} a {df_with_dates['fecha'].max()}")
    print(f"Herramientas: {resumen_general.columns[1:]}")
    
    return resumen_general, resumen_unicos, detalle_general, detalle_unicos

def add_date_column(df):
    date_formats = [
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y",
    ]
    
    for date_format in date_formats:
        try:
            df_with_dates = df.with_columns([
                pl.col('fechagestion')
                .str.to_datetime(format=date_format, strict=False)
                .dt.date()
                .alias('fecha')
            ]).drop_nulls(['fecha', 'perfil', 'cuenta_promesa'])
            
            if df_with_dates.height > 0:
                return df_with_dates
        except:
            continue
    
    try:
        df_with_dates = df.with_columns([
            pl.col('fechagestion')
            .str.slice(0, 10)
            .alias('fecha')
        ]).drop_nulls(['fecha', 'perfil', 'cuenta_promesa'])
        return df_with_dates
    except:
        return None