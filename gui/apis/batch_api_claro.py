import polars as pl
import pandas as pd
import os
import re
import chardet
from datetime import datetime
from ..apis.upload_batch import upload_batch_file

def process_batch_files(input_path, output_path):
    print(f"🔍 Scanning folder: {input_path}...")
    files = [f for f in os.listdir(input_path) if f.endswith(('.csv', '.xlsx'))]
    
    mapping_df = None
    data_payloads = []

    for fname in files:
        if any(x in fname for x in ["final_batch", "consolidado"]): continue
        fpath = os.path.join(input_path, fname)
        
        if fname.endswith('.csv'):
            try:
                with open(fpath, 'rb') as f:
                    raw_data = f.read(10000)
                    encoding = chardet.detect(raw_data)['encoding'] or 'latin-1'
                
                encodings_to_try = [encoding, 'utf-8', 'latin-1', 'iso-8859-1', 'cp1252', 'utf-8-sig']
                
                df_temp = None
                for enc in encodings_to_try:
                    try:
                        df_temp = pl.read_csv(
                            fpath, 
                            separator=';', 
                            infer_schema_length=0, 
                            ignore_errors=True, 
                            truncate_ragged_lines=True,
                            encoding=enc
                        )
                        if len(df_temp.columns) > 1:
                            break
                    except:
                        continue
                
                if df_temp is None:
                    with open(fpath, 'r', encoding='latin-1', errors='ignore') as f:
                        content = f.read()
                    from io import StringIO
                    df_temp = pl.read_csv(StringIO(content), separator=';', infer_schema_length=0, ignore_errors=True)
                
                cols = [c.strip() for c in df_temp.columns]
                
                if 'Multiproducto' in cols and 'Liquidacion' in cols:
                    print(f"💎 Mapping file detected: {fname}")
                    mapping_df = df_temp.select([
                        pl.col('Documento').alias('Documento'),
                        pl.col('Cuenta_Next').alias('Cuenta_Next')
                    ]).unique()
                    
                    mapping_df = mapping_df.with_columns([
                        pl.col('Cuenta_Next').cast(pl.Utf8).str.replace_all('-', '').str.strip_chars(),
                        pl.col('Documento').cast(pl.Utf8).str.replace_all(r'\D', '')
                    ])
                    continue
            except Exception as e:
                print(f"⚠️ Could not read {fname} for mapping: {e}")
        data_payloads.append(fname)

    final_dfs = []
    status_map = {'ANSWERED': 'CONTESTADA', 'NO ANSWER': 'NO CONTESTA', 'BUZON': 'BUZON DE VOZ', 'BUSY': 'OCUPADO', 'FAILED': 'FALLIDA'}

    def normalize_column_names(df):
        column_mapping = {}
        used_names = set()
        
        for col in df.columns:
            col_lower = col.lower().strip()
            new_name = None
            
            if any(x in col_lower for x in ['identificacion', 'documento', 'identificación']):
                new_name = 'identificacion'
            elif 'cuenta_next' in col_lower:
                new_name = 'cuenta_next'
            elif any(x in col_lower for x in ['cuenta_promesa', 'cuenta', 'referencia']):
                new_name = 'cuenta_promesa'
            elif 'dato_contacto' in col_lower:
                new_name = 'dato_contacto'
            elif any(x in col_lower for x in ['celular', 'numero', 'telefono', 'cel']):
                new_name = 'celular'
            elif any(x in col_lower for x in ['fecha_inicio', 'fecha inicio']):
                new_name = 'fecha_inicio_llamada'
            elif any(x in col_lower for x in ['fecha_fin', 'fecha fin']):
                new_name = 'fecha_fin_llamada'
            elif 'resultado' in col_lower and 'llamada' in col_lower:
                new_name = 'resultado_llamada'
            elif any(x in col_lower for x in ['segundo', 'duracion', 'seconds', 'secs', 'secound']):
                new_name = 'segundos'
            elif any(x in col_lower for x in ['mejor_marcacion', 'mejor marcacion']):
                new_name = 'mejor_marcacion'
            elif 'estado' in col_lower:
                new_name = 'estado'
            elif 'nombre_campana' in col_lower:
                new_name = 'nombre_campana'
            elif 'duracion' in col_lower:
                new_name = 'duracion'
            elif 'canal' in col_lower:
                new_name = 'canal'
            elif any(x in col_lower for x in ['email', 'correo']):
                new_name = 'email'
            elif any(x in col_lower for x in ['texto', 'mensaje', 'sms']):
                new_name = 'texto'
            elif 'numero marcado' in col_lower:
                new_name = 'numero_marcado'
            
            if new_name:
                if new_name in used_names:
                    count = 1
                    while f"{new_name}_{count}" in used_names:
                        count += 1
                    new_name = f"{new_name}_{count}"
                column_mapping[col] = new_name
                used_names.add(new_name)
            else:
                column_mapping[col] = col_lower.replace(' ', '_')
        
        return df.rename(column_mapping)

    def safe_read_csv(fpath):
        encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252', 'utf-8-sig']
        separators = [';', ',', '\t', '|']
        
        try:
            with open(fpath, 'rb') as f:
                raw_data = f.read(10000)
                detected = chardet.detect(raw_data)
                if detected['encoding']:
                    encodings.insert(0, detected['encoding'])
        except:
            pass
        
        for sep in separators:
            for enc in encodings:
                try:
                    df = pl.read_csv(
                        fpath, 
                        separator=sep, 
                        infer_schema_length=0, 
                        ignore_errors=True,
                        truncate_ragged_lines=True,
                        encoding=enc
                    )
                    if len(df.columns) > 1:
                        df.columns = [str(c).strip() for c in df.columns]
                        return df
                except:
                    continue
        return None

    def format_datetime_with_T(dt_str):
        if not dt_str or pd.isna(dt_str):
            return None
        
        dt_str = str(dt_str).strip()
        if not dt_str:
            return None
        
        formats = [
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d %H:%M:%S',
            '%d/%m/%Y %H:%M:%S',
            '%d-%m-%Y %H:%M:%S',
            '%Y%m%d %H:%M:%S',
            '%Y%m%dT%H:%M:%S',
            '%Y-%m-%d %H:%M',
            '%Y-%m-%d',
            '%d/%m/%Y',
            '%d-%m-%Y',
        ]
        
        for fmt in formats:
            try:
                dt_obj = datetime.strptime(dt_str, fmt)
                return dt_obj.strftime('%Y-%m-%dT%H:%M:%S')
            except:
                continue
        
        try:
            date_match = re.search(r'(\d{4}[-/]\d{2}[-/]\d{2})', dt_str)
            time_match = re.search(r'(\d{2}:\d{2}:\d{2})', dt_str)
            
            if date_match and time_match:
                date_part = date_match.group(1).replace('/', '-')
                time_part = time_match.group(1)
                return f"{date_part}T{time_part}"
            elif date_match:
                return f"{date_match.group(1).replace('/', '-')}T00:00:00"
        except:
            pass
        
        return dt_str

    def format_segundos(seg_val):
        if seg_val is None or pd.isna(seg_val):
            return "0"
        
        seg_str = str(seg_val).strip()
        if not seg_str:
            return "0"
        
        if ':' in seg_str:
            try:
                parts = seg_str.split(':')
                if len(parts) == 3:
                    hours, minutes, seconds = parts
                    total_seconds = int(hours) * 3600 + int(minutes) * 60 + int(float(seconds))
                    return str(total_seconds)
                elif len(parts) == 2:
                    minutes, seconds = parts
                    total_seconds = int(minutes) * 60 + int(float(seconds))
                    return str(total_seconds)
            except:
                pass
        
        try:
            num_val = float(seg_str)
            return str(int(num_val))
        except:
            return "0"

    def extract_date_from_filename(fname):
        date_match = re.search(r'(\d{8})', fname)
        if date_match:
            date_str = date_match.group(1)
            try:
                dt_obj = datetime.strptime(date_str, '%d%m%Y')
                return dt_obj.strftime('%Y-%m-%dT11:00:00')
            except:
                try:
                    dt_obj = datetime.strptime(date_str, '%Y%m%d')
                    return dt_obj.strftime('%Y-%m-%dT11:00:00')
                except:
                    pass
        return datetime.now().strftime('%Y-%m-%dT11:00:00')

    def safe_get_column(df, possible_names):
        for name in possible_names:
            if name in df.columns:
                return name
        return None

    for fname in data_payloads:
        fpath = os.path.join(input_path, fname)
        try:
            es_email_file = any(x in fname.upper() for x in ["EMAIL", "CORREO"])
            
            if fname.endswith('.xlsx'):
                try:
                    df = pl.from_pandas(pd.read_excel(fpath))
                    df.columns = [str(c).strip() for c in df.columns]
                except:
                    print(f"⚠️ Could not read Excel {fname}, skipping")
                    continue
            else:
                df = safe_read_csv(fpath)
                if df is None:
                    print(f"⚠️ Could not read {fname}, skipping")
                    continue

            df = normalize_column_names(df)
            cols = df.columns
            res = None
            conditional = "Unknown"

            if es_email_file and 'canal' in cols:
                try:
                    fecha_base = extract_date_from_filename(fname)
                    df_email = df.filter(pl.col('canal').cast(pl.Utf8).str.to_uppercase() == 'EMAIL')
                    
                    if df_email.height > 0:
                        id_col = safe_get_column(df_email, ['identificacion'])
                        cuenta_col = safe_get_column(df_email, ['cuenta_promesa', 'cuenta_next'])
                        dato_col = safe_get_column(df_email, ['dato_contacto', 'email'])
                        
                        if id_col and cuenta_col and dato_col:
                            res = df_email.select([
                                (pl.lit("Asunto: INFORMACION IMPORTANTE FACTURACION CLARO") + pl.lit("|") + pl.col(dato_col).cast(pl.Utf8)).alias('gestion'),
                                pl.lit("envios@recuperasas.com").alias('usuario'),
                                pl.lit(fecha_base).alias('fechagestion'),
                                pl.lit("Envio manual Syncra").alias('accion'),
                                pl.lit("CORREO MASIVIAN").alias('perfil'),
                                pl.col(dato_col).cast(pl.Utf8).alias('demografico'),
                                pl.col(id_col).cast(pl.Utf8).str.replace_all(r'\D', '').alias('identificacion'),
                                (pl.col(cuenta_col).cast(pl.Utf8) + "-").alias('cuenta_promesa'),
                                pl.lit("claro").alias('campana')
                            ])
                            conditional = "CORREO MASIVIAN"
                except Exception as e:
                    print(f"⚠️ Error in EMAIL processing for {fname}: {e}")

            if res is None and 'texto' in cols and 'dato_contacto' in cols:
                try:
                    date_match = re.search(r'(\d{8})_(\d{4})', fname)
                    dt_str = extract_date_from_filename(fname) if not date_match else datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
                    
                    cuenta_col = safe_get_column(df, ['cuenta_promesa', 'cuenta_next'])
                    if not cuenta_col:
                        cuenta_col = 'cuenta_promesa'
                        df = df.with_columns(pl.lit("").alias('cuenta_promesa'))
                    
                    res = df.select([
                        pl.col('texto').cast(pl.Utf8).alias('gestion'),
                        pl.lit("87910__anthony.quiva239").alias('usuario'),
                        pl.lit(dt_str).alias('fechagestion'),
                        pl.lit("Envio manual Syncra").alias('accion'),
                        pl.lit("MENSAJERIA SAEM").alias('perfil'),
                        pl.col('dato_contacto').cast(pl.Utf8).alias('demografico'),
                        pl.col('identificacion').cast(pl.Utf8).alias('identificacion'),
                        (pl.col(cuenta_col).cast(pl.Utf8) + "-").alias('cuenta_promesa'),
                        pl.lit("claro").alias('campana')
                    ])
                    conditional = "SMS Saem"
                except Exception as e:
                    print(f"⚠️ Error in SMS processing for {fname}: {e}")

            if res is None and mapping_df is not None:
                try:
                    id_col = safe_get_column(df, ['identificacion'])
                    if id_col:
                        df = df.with_columns(pl.col(id_col).cast(pl.Utf8).str.strip_chars())
                        joined = df.join(mapping_df, left_on=id_col, right_on='Cuenta_Next', how='inner')
                        
                        if not joined.is_empty():
                            if 'estado' in cols:
                                fecha_col = safe_get_column(joined, ['fecha_inicio_llamada'])
                                fecha_formateada = joined[fecha_col].map_elements(format_datetime_with_T, return_dtype=pl.Utf8) if fecha_col else pl.lit(datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
                                
                                nombre_campana_col = safe_get_column(joined, ['nombre_campana']) or 'nombre_campana'
                                if nombre_campana_col not in joined.columns:
                                    joined = joined.with_columns(pl.lit("BLASTER").alias('nombre_campana'))
                                
                                segundos_col = safe_get_column(joined, ['segundos', 'duracion'])
                                segundos_formateados = joined[segundos_col].map_elements(format_segundos, return_dtype=pl.Utf8) if segundos_col else pl.lit("0")
                                
                                numero_marcado_col = safe_get_column(joined, ['numero_marcado']) or 'numero_marcado'
                                if numero_marcado_col not in joined.columns:
                                    joined = joined.with_columns(pl.lit("").alias('numero_marcado'))
                                
                                res = joined.select([
                                    (pl.col('estado').cast(pl.Utf8).replace(status_map) + 
                                     " - " + pl.col(nombre_campana_col).cast(pl.Utf8).fill_null("BLASTER") + 
                                     " - Duracion: " + segundos_formateados).alias('gestion'),
                                    pl.lit("Caller ID rotativo").alias('usuario'),
                                    fecha_formateada.alias('fechagestion'),
                                    pl.lit("Ejecucion del Blaster").alias('accion'),
                                    pl.lit("BLASTER CONTROLNEXT").alias('perfil'),
                                    pl.col(numero_marcado_col).cast(pl.Utf8).alias('demografico'),
                                    pl.col('Documento').cast(pl.Utf8).alias('identificacion'),
                                    (pl.col(id_col).cast(pl.Utf8) + "-").alias('cuenta_promesa'),
                                    pl.lit("claro").alias('campana')
                                ])
                                conditional = "Blaster"
                            
                            elif 'mejor_marcacion' in cols:
                                fecha_col = safe_get_column(joined, ['fecha_inicio_llamada'])
                                fecha_formateada = joined[fecha_col].map_elements(format_datetime_with_T, return_dtype=pl.Utf8) if fecha_col else pl.lit(datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))

                                segundos_col = safe_get_column(joined, ['segundos'])
                                segundos_formateados = joined[segundos_col].map_elements(format_segundos, return_dtype=pl.Utf8) if segundos_col else pl.lit("0")
                                
                                celular_col = safe_get_column(joined, ['celular']) or 'celular'
                                if celular_col not in joined.columns:
                                    joined = joined.with_columns(pl.lit("").alias('celular'))

                                res = joined.select([
                                    (pl.col('mejor_marcacion').cast(pl.Utf8) + " - Duracion: " + segundos_formateados).alias('gestion'),
                                    pl.lit("Caller ID rotativo").alias('usuario'),
                                    fecha_formateada.alias('fechagestion'),
                                    pl.lit("Envio manual Syncra").alias('accion'),
                                    pl.lit("IVR SAEM").alias('perfil'),
                                    pl.col(celular_col).cast(pl.Utf8).str.slice(-10).alias('demografico'),
                                    pl.col('Documento').cast(pl.Utf8).alias('identificacion'),
                                    (pl.col(id_col).cast(pl.Utf8) + "-").alias('cuenta_promesa'),
                                    pl.lit("claro").alias('campana')
                                ])
                                conditional = "IVR Saem"
                except Exception as e:
                    print(f"⚠️ Error in Blaster/IVR processing for {fname}: {e}")

            if res is not None:
                res = res.select([pl.all().cast(pl.Utf8)])
                final_dfs.append(res)
                print(f"✅ {fname} processed as {conditional}")
            else:
                print(f"⏭️ {fname} skipped - no matching processing logic")

        except Exception as e:
            print(f"❌ Fatal error in {fname}: {e}")

    if final_dfs:
        output_df = pl.concat(final_dfs)
        output_df = output_df.drop_nulls(subset=['fechagestion', 'demografico', 'identificacion', 'cuenta_promesa']).unique()
        
        print("\n📊 --- SUMMARY BY PROFILE ---")
        print(output_df.group_by("perfil").len(name="count"))
        print(f"Total final records: {output_df.height:,}")

        out_file = os.path.join(output_path, f"batch_api_claro_{datetime.now().strftime('%Y%m%d_%H%M')}.csv")
        output_df.write_csv(out_file, separator=';')

        try:
            job_result = upload_batch_file(out_file)
            if job_result and job_result.get('jobId'):
                job_id = job_result.get('jobId')
                print(f"📤 File sent to API - Job ID: {job_id}")
                return f"Archivo batch cargado bajo el Job ID: {job_id}"
            else:
                print("⚠️ File saved but could not be sent to API")
                return f"Archivo guardado con un novedad sobre API"
        except Exception as e:
            print(f"⚠️ Error uploading to API: {e}")
            return f"Archivo guardado con error en API"
    
    return "Nothing processed."