import polars as pl
from pathlib import Path
from datetime import datetime
import traceback
import functools
print = functools.partial(print, flush=True)

COLUMN_CONFIG = {
    'BLASTER': {
        'CANAL': 'BLASTER INTERCOM',
        'ESTADO_TRANSLATIONS': {
            'ANSWERED': 'CONTESTADA',
            'NO ANSWER': 'NO CONTESTA',
            'BUZON': 'BUZON DE VOZ',
            'BUSY': 'OCUPADO',
            'FAILED': 'FALLIDA'
        }
    },
    'IVR_SAEM': {
        'CANAL': 'IVR SAEM',
        'ESTADO_TRANSLATIONS': {
            'ANSWERED': 'CONTESTADA',
            'NO ANSWER': 'NO CONTESTA',
            'BUZON': 'BUZON DE VOZ',
            'BUSY': 'OCUPADO',
            'FAILED': 'FALLIDA',
            'Fallo': 'FALLIDA',
            'Satisfactorio': 'CONTESTADA',
            'Colgo': 'COLGO',
            'No Contesta': 'NO CONTESTA',
            'Maquina': 'BUZON DE VOZ',
            'Congestion': 'OCUPADO',
            'Numero Telefonico Invalido': 'FALLIDA'
        }
    }
}

COLUMNS_TARGET = [
    'CANAL', 'ESTADO', 'FECHA_INICIO_ULTIMA_LLAMADA', 'DURACION_SEGUNDOS',
    'CELULAR', 'IDENTIFICACION', 'MARCA', 'CRM_ORIGEN', 'TIPO_BASE',
    'FECHA_INGRESO', 'RANGO_SALDO', 'NOMBRE_CAMPANA', 'NOMBRE DE LA CAMPAÑA'
]

def read_csv_with_fallback(filepath):
    for sep in [';', ',', '\t']:
        try:
            df = pl.read_csv(filepath, separator=sep, infer_schema_length=1000, 
                           try_parse_dates=True, ignore_errors=True)
            if len(df.columns) > 1:
                return df
        except:
            continue
    raise ValueError(f"No se pudo leer {filepath}")

def normalize_blaster_columns(df):
    rename_map = {}
    for col in df.columns:
        col_clean = col.strip()
        if 'FECHA DE MARCACION' in col_clean.upper():
            rename_map[col] = 'FECHA DE MARCACION'
        elif 'DURACION' in col_clean.upper():
            rename_map[col] = 'DURACION'
    
    if rename_map:
        df = df.rename(rename_map)
    
    if 'IDENTIFICACION' in df.columns:
        df = df.rename({'IDENTIFICACION': 'identificacion'})
    elif 'IDENTI' in df.columns:
        df = df.rename({'IDENTI': 'identificacion'})
    
    return df

def normalize_ivr_columns(df):
    if 'Identificacion' in df.columns:
        df = df.rename({'Identificacion': 'identificacion'})
    
    if 'Celular' in df.columns:
        df = df.with_columns(
            pl.col('Celular').cast(pl.Utf8)
            .str.replace_all('"', '')
            .str.slice(2)
            .alias('Celular')
        )
    
    return df

def normalize_assignment_columns(df):
    rename_map = {
        'Cuenta_Next': 'cuenta_next',
        'Fecha_Ingreso': 'fecha_ingreso',
        'Tipo_Base': 'tipo_base',
        'Marca_Asignada': 'marca',
        'CRM_Origen': 'crm_origen',
        'Rango_Deuda': 'rango_saldo',
        'Nombre Campana': 'nombre_campana'
    }
    
    existing_rename = {k: v for k, v in rename_map.items() if k in df.columns}
    if existing_rename:
        df = df.rename(existing_rename)
    
    return df

def format_duration_to_seconds(duration):
    if duration is None:
        return 0
    
    try:
        duration_str = str(duration).strip()
        
        if duration_str.replace('.', '', 1).replace('-', '', 1).isdigit():
            return int(float(duration_str))
        
        parts = duration_str.split(':')
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(float(parts[2]))
        elif len(parts) == 2:
            return int(parts[0]) * 60 + int(float(parts[1]))
    except:
        pass
    
    return 0

def clean_datetime_string(date_str):
    if date_str is None:
        return ''
    
    date_str = str(date_str)
    if '.000000' in date_str:
        return date_str.replace('.000000', '')
    return date_str

def process_blaster_to_common_format(df_blaster, df_assignment):
    assignment_cols = ['cuenta_next', 'marca', 'crm_origen', 'tipo_base', 
                      'fecha_ingreso', 'rango_saldo', 'nombre_campana']
    existing_assignment_cols = [col for col in assignment_cols if col in df_assignment.columns]
    
    df_joined = df_blaster.join(
        df_assignment.select(existing_assignment_cols),
        left_on='identificacion',
        right_on='cuenta_next',
        how='inner'
    )
    
    if df_joined.height == 0:
        return None
    
    if 'DURACION' in df_joined.columns and 'ESTADO' in df_joined.columns:
        df_joined = df_joined.with_columns([
            pl.col('DURACION')
            .map_elements(format_duration_to_seconds, return_dtype=pl.Int64)
            .alias('_dur_sec')
        ]).with_columns(
            pl.when(
                (pl.col('ESTADO').is_in(['CONTESTADA', 'COLGO'])) & 
                (pl.col('_dur_sec') < 5)
            )
            .then(pl.lit('CONTESTADA PARCIAL'))
            .otherwise(pl.col('ESTADO'))
            .alias('ESTADO')
        ).drop('_dur_sec')
    
    if 'FECHA DE MARCACION' in df_joined.columns:
        df_joined = df_joined.with_columns(
            pl.col('FECHA DE MARCACION')
            .map_elements(clean_datetime_string, return_dtype=pl.Utf8)
            .alias('FECHA DE MARCACION')
        )
    
    columns = []
    columns.append(pl.lit(COLUMN_CONFIG['BLASTER']['CANAL']).cast(pl.Utf8).alias('CANAL'))
    columns.append(pl.col('ESTADO').cast(pl.Utf8).alias('ESTADO') if 'ESTADO' in df_joined.columns else pl.lit('').cast(pl.Utf8).alias('ESTADO'))
    columns.append(pl.col('FECHA DE MARCACION').cast(pl.Utf8).alias('FECHA_INICIO_ULTIMA_LLAMADA') if 'FECHA DE MARCACION' in df_joined.columns else pl.lit('').cast(pl.Utf8).alias('FECHA_INICIO_ULTIMA_LLAMADA'))
    
    if 'DURACION' in df_joined.columns:
        columns.append(
            pl.col('DURACION')
            .map_elements(format_duration_to_seconds, return_dtype=pl.Int64)
            .cast(pl.Int64)
            .alias('DURACION_SEGUNDOS')
        )
    else:
        columns.append(pl.lit(0).cast(pl.Int64).alias('DURACION_SEGUNDOS'))
    
    if 'NUMERO MARCADO' in df_joined.columns:
        columns.append(pl.col('NUMERO MARCADO').cast(pl.Utf8).alias('CELULAR'))
    elif 'TELEFONO 1' in df_joined.columns:
        columns.append(pl.col('TELEFONO 1').cast(pl.Utf8).alias('CELULAR'))
    else:
        columns.append(pl.lit('').cast(pl.Utf8).alias('CELULAR'))
    
    columns.append((pl.col('identificacion').cast(pl.Utf8) + '-').alias('IDENTIFICACION'))
    columns.append(pl.col('marca').cast(pl.Utf8).alias('MARCA') if 'marca' in df_joined.columns else pl.lit('').cast(pl.Utf8).alias('MARCA'))
    columns.append(pl.col('crm_origen').cast(pl.Utf8).alias('CRM_ORIGEN') if 'crm_origen' in df_joined.columns else pl.lit('').cast(pl.Utf8).alias('CRM_ORIGEN'))
    columns.append(pl.col('tipo_base').cast(pl.Utf8).alias('TIPO_BASE') if 'tipo_base' in df_joined.columns else pl.lit('').cast(pl.Utf8).alias('TIPO_BASE'))
    columns.append(pl.col('fecha_ingreso').cast(pl.Utf8).alias('FECHA_INGRESO') if 'fecha_ingreso' in df_joined.columns else pl.lit('').cast(pl.Utf8).alias('FECHA_INGRESO'))
    columns.append(pl.col('rango_saldo').cast(pl.Utf8).alias('RANGO_SALDO') if 'rango_saldo' in df_joined.columns else pl.lit('').cast(pl.Utf8).alias('RANGO_SALDO'))
    columns.append(pl.col('nombre_campana').cast(pl.Utf8).alias('NOMBRE_CAMPANA') if 'nombre_campana' in df_joined.columns else pl.lit('').cast(pl.Utf8).alias('NOMBRE_CAMPANA'))
    
    if 'NOMBRE DE LA CAMPAÑA' in df_joined.columns:
        columns.append(pl.col('NOMBRE DE LA CAMPAÑA').cast(pl.Utf8).alias('NOMBRE DE LA CAMPAÑA'))
    else:
        columns.append(pl.lit('').cast(pl.Utf8).alias('NOMBRE DE LA CAMPAÑA'))
    
    return df_joined.select(columns)

def process_ivr_to_common_format(df_ivr, df_assignment):
    assignment_cols = ['cuenta_next', 'marca', 'crm_origen', 'tipo_base', 
                      'fecha_ingreso', 'rango_saldo', 'nombre_campana']
    existing_assignment_cols = [col for col in assignment_cols if col in df_assignment.columns]
    
    df_joined = df_ivr.join(
        df_assignment.select(existing_assignment_cols),
        left_on='identificacion',
        right_on='cuenta_next',
        how='inner'
    )
    
    if df_joined.height == 0:
        return None
    
    if 'Mejor_Marcacion' in df_joined.columns:
        df_joined = df_joined.with_columns(
            pl.when(pl.col('Mejor_Marcacion').is_in(COLUMN_CONFIG['IVR_SAEM']['ESTADO_TRANSLATIONS'].keys()))
            .then(pl.col('Mejor_Marcacion').replace(COLUMN_CONFIG['IVR_SAEM']['ESTADO_TRANSLATIONS']))
            .otherwise(pl.col('Mejor_Marcacion'))
            .alias('ESTADO')
        )
    else:
        df_joined = df_joined.with_columns(pl.lit('').cast(pl.Utf8).alias('ESTADO'))
    
    if 'secounds' in df_joined.columns and 'ESTADO' in df_joined.columns:
        df_joined = df_joined.with_columns(
            pl.when(
                (pl.col('ESTADO').is_in(['CONTESTADA', 'COLGO'])) & 
                (pl.col('secounds').fill_null(0) < 5)
            )
            .then(pl.lit('CONTESTADA PARCIAL'))
            .otherwise(pl.col('ESTADO'))
            .alias('ESTADO')
        )
    
    if 'Fecha_Inicio_Ultima_Llamada' in df_joined.columns:
        df_joined = df_joined.with_columns(
            pl.col('Fecha_Inicio_Ultima_Llamada')
            .map_elements(clean_datetime_string, return_dtype=pl.Utf8)
            .alias('Fecha_Inicio_Ultima_Llamada')
        )
    
    columns = []
    columns.append(pl.lit(COLUMN_CONFIG['IVR_SAEM']['CANAL']).cast(pl.Utf8).alias('CANAL'))
    columns.append(pl.col('ESTADO').cast(pl.Utf8).alias('ESTADO'))
    columns.append(pl.col('Fecha_Inicio_Ultima_Llamada').cast(pl.Utf8).alias('FECHA_INICIO_ULTIMA_LLAMADA') if 'Fecha_Inicio_Ultima_Llamada' in df_joined.columns else pl.lit('').cast(pl.Utf8).alias('FECHA_INICIO_ULTIMA_LLAMADA'))
    
    if 'secounds' in df_joined.columns:
        columns.append(
            pl.col('secounds')
            .cast(pl.Int64)
            .fill_null(0)
            .cast(pl.Int64)
            .alias('DURACION_SEGUNDOS')
        )
    else:
        columns.append(pl.lit(0).cast(pl.Int64).alias('DURACION_SEGUNDOS'))
    
    columns.append(pl.col('Celular').cast(pl.Utf8).alias('CELULAR') if 'Celular' in df_joined.columns else pl.lit('').cast(pl.Utf8).alias('CELULAR'))
    columns.append((pl.col('identificacion').cast(pl.Utf8) + '-').alias('IDENTIFICACION'))
    columns.append(pl.col('marca').cast(pl.Utf8).alias('MARCA') if 'marca' in df_joined.columns else pl.lit('').cast(pl.Utf8).alias('MARCA'))
    columns.append(pl.col('crm_origen').cast(pl.Utf8).alias('CRM_ORIGEN') if 'crm_origen' in df_joined.columns else pl.lit('').cast(pl.Utf8).alias('CRM_ORIGEN'))
    columns.append(pl.col('tipo_base').cast(pl.Utf8).alias('TIPO_BASE') if 'tipo_base' in df_joined.columns else pl.lit('').cast(pl.Utf8).alias('TIPO_BASE'))
    columns.append(pl.col('fecha_ingreso').cast(pl.Utf8).alias('FECHA_INGRESO') if 'fecha_ingreso' in df_joined.columns else pl.lit('').cast(pl.Utf8).alias('FECHA_INGRESO'))
    columns.append(pl.col('rango_saldo').cast(pl.Utf8).alias('RANGO_SALDO') if 'rango_saldo' in df_joined.columns else pl.lit('').cast(pl.Utf8).alias('RANGO_SALDO'))
    columns.append(pl.col('nombre_campana').cast(pl.Utf8).alias('NOMBRE_CAMPANA') if 'nombre_campana' in df_joined.columns else pl.lit('').cast(pl.Utf8).alias('NOMBRE_CAMPANA'))
    columns.append(pl.lit('ivr masivo').cast(pl.Utf8).alias('NOMBRE DE LA CAMPAÑA'))
    
    return df_joined.select(columns)

def process_ivr_data(input_folder: str, output_folder: str):
    print(f"\n{'='*60}")
    print(f"CONSOLIDADO BLASTER + IVR SAEM")
    print(f"{'='*60}")
    
    input_path = Path(input_folder)
    output_path = Path(output_folder)
    
    if not input_path.exists():
        print(f"❌ ERROR: Carpeta de entrada no existe")
        return
    
    output_path.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    csv_files = list(input_path.glob("*.csv"))
    print(f"Archivos: {len(csv_files)}")
    
    df_blaster_raw = None
    df_ivr_raw = None
    df_assignment = None
    
    for csv_file in csv_files:
        try:
            df = read_csv_with_fallback(csv_file)
            
            if "asignacion_claro" in csv_file.name.lower():
                df = normalize_assignment_columns(df)
                if 'cuenta_next' in df.columns:
                    df = df.with_columns(
                        pl.col('cuenta_next').cast(pl.Utf8)
                        .str.replace_all('["-]', '')
                        .alias('cuenta_next')
                    )
                    df_assignment = df if df_assignment is None else pl.concat([df_assignment, df], how="diagonal")
            
            elif "LEAD ID" in str(df.columns) or any('LEAD ID' in str(col) for col in df.columns):
                df = normalize_blaster_columns(df)
                if 'identificacion' in df.columns:
                    df = df.with_columns(
                        pl.col('identificacion').cast(pl.Utf8)
                        .str.replace_all('["-]', '')
                        .alias('identificacion')
                    )
                    if 'ESTADO' in df.columns:
                        df = df.with_columns(
                            pl.when(pl.col('ESTADO').is_in(COLUMN_CONFIG['BLASTER']['ESTADO_TRANSLATIONS'].keys()))
                            .then(pl.col('ESTADO').replace(COLUMN_CONFIG['BLASTER']['ESTADO_TRANSLATIONS']))
                            .otherwise(pl.col('ESTADO'))
                            .alias('ESTADO')
                        )
                    df_blaster_raw = df if df_blaster_raw is None else pl.concat([df_blaster_raw, df], how="diagonal")
            
            elif ('Identificacion' in df.columns or 'identificacion' in df.columns) and 'Celular' in df.columns:
                df = normalize_ivr_columns(df)
                if 'identificacion' in df.columns:
                    df = df.with_columns(
                        pl.col('identificacion').cast(pl.Utf8)
                        .str.replace_all('["-]', '')
                        .alias('identificacion')
                    )
                    df_ivr_raw = df if df_ivr_raw is None else pl.concat([df_ivr_raw, df], how="diagonal")
                    
        except Exception as e:
            continue
    
    if df_assignment is None:
        print(f"❌ ERROR: No hay archivo de asignación")
        return
    
    all_dfs = []
    
    if df_blaster_raw is not None:
        print(f"📊 Blaster: {df_blaster_raw.height:,} registros")
        df_blaster = process_blaster_to_common_format(df_blaster_raw, df_assignment)
        if df_blaster is not None:
            all_dfs.append(df_blaster)
            print(f"  ✅ Match: {df_blaster.height:,}")
    
    if df_ivr_raw is not None:
        print(f"📊 IVR SAEM: {df_ivr_raw.height:,} registros")
        df_ivr = process_ivr_to_common_format(df_ivr_raw, df_assignment)
        if df_ivr is not None:
            all_dfs.append(df_ivr)
            print(f"  ✅ Match: {df_ivr.height:,}")
    
    if all_dfs:
        df_final = pl.concat(all_dfs, how="vertical")
        df_final = df_final.select([col for col in COLUMNS_TARGET if col in df_final.columns])
        
        output_file = output_path / f'reporte_consolidado_blaster_ivr_{timestamp}.csv'
        df_final.write_csv(output_file, separator=';', quote_style='never')
        
        print(f"\n✅ Reporte general: {output_file.name}")
        print(f"📊 Total: {df_final.height:,} registros")
        
        df_efectivo = df_final.filter(pl.col('ESTADO').is_in(['CONTESTADA', 'COLGO', 'CONTESTADA PARCIAL']))
        
        if df_efectivo.height > 0:
            output_efectivo = output_path / f'reporte_efectivo_blaster_ivr_{timestamp}.csv'
            df_efectivo.write_csv(output_efectivo, separator=';', quote_style='never')
            print(f"✅ Reporte efectivo: {output_efectivo.name}")
            print(f"📊 Total efectivo: {df_efectivo.height:,} registros")
        
        canal_counts = df_final['CANAL'].value_counts()
        for row in canal_counts.iter_rows():
            print(f"   • {row[0]}: {row[1]:,} ({row[1]/df_final.height*100:.1f}%)")
    
    print(f"\n{'='*60}")
    print(f"PROCESO COMPLETADO")
    print(f"{'='*60}")