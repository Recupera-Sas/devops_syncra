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
                
                df_temp = pl.read_csv(
                    fpath, 
                    separator=';', 
                    infer_schema_length=0, 
                    ignore_errors=True, 
                    truncate_ragged_lines=True,
                    encoding=encoding
                )
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
                        return df
                except:
                    continue
        
        try:
            with open(fpath, 'r', encoding='latin-1', errors='ignore') as f:
                content = f.read()
            from io import StringIO
            df = pl.read_csv(StringIO(content), separator=';', infer_schema_length=0, ignore_errors=True)
            return df
        except:
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

    def extract_date_from_filename(fname):
        date_match = re.search(r'(\d{8})', fname)
        if date_match:
            date_str = date_match.group(1)
            try:
                dt_obj = datetime.strptime(date_str, '%d%m%Y')
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
                except:
                    print(f"⚠️ Could not read Excel {fname}, skipping")
                    continue
            else:
                df = safe_read_csv(fpath)
                if df is None:
                    print(f"⚠️ Could not read {fname} with any encoding/separator, skipping")
                    continue

            df.columns = [str(c).strip() for c in df.columns]
            cols = df.columns
            res = None
            conditional = "Unknown"

            if es_email_file and 'Canal' in cols:
                try:
                    fecha_base = extract_date_from_filename(fname)
                    
                    df_email = df.filter(pl.col('Canal').cast(pl.Utf8).str.to_uppercase() == 'EMAIL')
                    
                    if df_email.height > 0:
                        id_col = safe_get_column(df_email, ['identificacion', 'Identificacion', 'IDENTIFICACION', 'Documento', 'documento', 'DOCUMENTO'])
                        cuenta_col = safe_get_column(df_email, ['Cuenta', 'cuenta', 'CUENTA', 'Referencia', 'referencia', 'REFERENCIA', 'Cuenta_Next'])
                        dato_col = safe_get_column(df_email, ['Dato_Contacto', 'dato_contacto', 'DATOCONTACTO', 'Email', 'email', 'EMAIL'])
                        
                        if id_col and cuenta_col and dato_col:
                            res = df_email.select([
                                pl.lit("Asunto: INFORMACION IMPORTANTE FACTURACION CLARO").alias('gestion'),
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
                            print(f"   📊 EMAIL records found: {df_email.height}")
                except Exception as e:
                    print(f"⚠️ Error in EMAIL processing for {fname}: {e}")

            if res is None and 'SMS' in cols and 'Dato_Contacto' in cols:
                try:
                    date_match = re.search(r'(\d{8})_(\d{4})', fname)
                    dt_str = None
                    if date_match:
                        try:
                            dt_obj = datetime.strptime(date_match.group(0), '%d%m%Y_%H%M')
                            dt_str = dt_obj.strftime('%Y-%m-%dT%H:%M:%S')
                        except:
                            dt_str = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
                    
                    res = df.select([
                        pl.col('SMS').cast(pl.Utf8).alias('gestion'),
                        pl.lit("87910__anthony.quiva239").alias('usuario'),
                        pl.lit(dt_str).alias('fechagestion'),
                        pl.lit("Envio manual Syncra").alias('accion'),
                        pl.lit("MENSAJERIA SAEM").alias('perfil'),
                        pl.col('Dato_Contacto').cast(pl.Utf8).alias('demografico'),
                        pl.col('Identificacion').cast(pl.Utf8).alias('identificacion'),
                        (pl.col("Cuenta_Next").cast(pl.Utf8) + "-").alias('cuenta_promesa'),
                        pl.lit("claro").alias('campana')
                    ])
                    conditional = "SMS Saem"
                except Exception as e:
                    print(f"⚠️ Error in SMS processing for {fname}: {e}")

            if res is None and mapping_df is not None:
                try:
                    id_col = safe_get_column(df, ['IDENTIFICACION', 'Identificacion', 'Identificación', 'identificacion'])
                    if id_col:
                        df = df.with_columns(pl.col(id_col).cast(pl.Utf8).str.strip_chars())
                        joined = df.join(mapping_df, left_on=id_col, right_on='Cuenta_Next', how='inner')
                        
                        if not joined.is_empty():
                            if 'ESTADO' in cols:
                                fecha_col = 'FECHA DE MARCACION'
                                if fecha_col in joined.columns:
                                    fecha_formateada = joined[fecha_col].map_elements(
                                        format_datetime_with_T, return_dtype=pl.Utf8
                                    )
                                else:
                                    fecha_formateada = pl.lit(datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
                                
                                res = joined.select([
                                    (pl.col('ESTADO').cast(pl.Utf8).replace(status_map) + 
                                     " - " + pl.col('NOMBRE DE LA CAMPAÑA').cast(pl.Utf8).fill_null("BLASTER") + 
                                     " - Duracion: " + pl.col('DURACION').cast(pl.Utf8)).alias('gestion'),
                                    pl.lit("Caller ID rotativo").alias('usuario'),
                                    fecha_formateada.alias('fechagestion'),
                                    pl.lit("Ejecucion del Blaster").alias('accion'),
                                    pl.lit("BLASTER CONTROLNEXT").alias('perfil'),
                                    pl.col('NUMERO MARCADO').cast(pl.Utf8).alias('demografico'),
                                    pl.col('Documento').cast(pl.Utf8).alias('identificacion'),
                                    (pl.col(id_col).cast(pl.Utf8) + "-").alias('cuenta_promesa'),
                                    pl.lit("claro").alias('campana')
                                ])
                                conditional = "Blaster"
                            
                            elif 'Mejor_Marcacion' in cols:
                                fecha_col = 'Fecha_Inicio_Ultima_Llamada'
                                if fecha_col in joined.columns:
                                    fecha_formateada = joined[fecha_col].map_elements(
                                        format_datetime_with_T, return_dtype=pl.Utf8
                                    )
                                else:
                                    fecha_formateada = pl.lit(datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))

                                res = joined.select([
                                    (pl.col('Mejor_Marcacion').cast(pl.Utf8) + 
                                     " - Duracion: " + pl.col('secounds').cast(pl.Utf8)).alias('gestion'),
                                    pl.lit("Caller ID rotativo").alias('usuario'),
                                    fecha_formateada.alias('fechagestion'),
                                    pl.lit("Envio manual Syncra").alias('accion'),
                                    pl.lit("IVR SAEM").alias('perfil'),
                                    pl.col('Celular').cast(pl.Utf8).str.slice(-10).alias('demografico'),
                                    pl.col('Documento').cast(pl.Utf8).alias('identificacion'),
                                    (pl.col(id_col).cast(pl.Utf8) + "-").alias('cuenta_promesa'),
                                    pl.lit("claro").alias('campana')
                                ])
                                conditional = "IVR Saem"
                except Exception as e:
                    print(f"⚠️ Error in Blaster/IVR processing for {fname}: {e}")

            if res is not None:
                try:
                    res = res.select([pl.all().cast(pl.Utf8)])
                    final_dfs.append(res)
                    print(f"✅ {fname} processed as {conditional}")
                except Exception as e:
                    print(f"⚠️ Error casting result for {fname}: {e}")
            else:
                print(f"⏭️ {fname} skipped - no matching processing logic")

        except Exception as e:
            print(f"❌ Fatal error in {fname}: {e}")

    if final_dfs:
        try:
            output_df = pl.concat(final_dfs)
            output_df = output_df.drop_nulls(subset=['fechagestion', 'demografico', 'identificacion', 'cuenta_promesa']).unique()
            
            print("\n📊 --- SUMMARY BY PROFILE ---")
            conteo = output_df.group_by("perfil").len(name="count")
            print(conteo)
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
        
        except Exception as e:
            error_msg = f"Error creating final output: {e}"
            print(f"❌ {error_msg}")
            return error_msg
    
    return "Nothing processed."