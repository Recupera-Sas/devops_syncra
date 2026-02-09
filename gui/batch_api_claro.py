import polars as pl
import pandas as pd
import os
import re
from datetime import datetime

def process_batch_files(input_path, output_path):
    print(f"🔍 Escaneando carpeta: {input_path}...")
    files = [f for f in os.listdir(input_path) if f.endswith(('.csv', '.xlsx'))]
    
    mapping_df = None
    data_payloads = []

    for fname in files:
        if any(x in fname for x in ["final_batch", "consolidado"]): continue
        fpath = os.path.join(input_path, fname)
        
        if fname.endswith('.csv'):
            try:
                df_temp = pl.read_csv(fpath, separator=';', infer_schema_length=0, ignore_errors=True)
                cols = [c.strip() for c in df_temp.columns]
                
                if 'responsable_cargue' in cols and 'Liquidacion' in cols:
                    print(f"💎 Cruce detectado: {fname}")
                    mapping_df = df_temp.select([
                        pl.col('Documento').alias('Documento'),
                        pl.col('Cuenta_Next').alias('Cuenta_Next')
                    ]).unique()
                    
                    mapping_df = mapping_df.with_columns([
                        pl.col('Cuenta_Next').cast(pl.Utf8).str.replace_all('-', '').str.strip_chars(),
                        pl.col('Documento').cast(pl.Utf8).str.replace_all(r'\D', '')
                    ])
                    continue
            except:
                pass
        data_payloads.append(fname)

    final_dfs = []
    status_map = {'ANSWERED': 'CONTESTADA', 'NO ANSWER': 'NO CONTESTA', 'BUZON': 'BUZON DE VOZ', 'BUSY': 'OCUPADO', 'FAILED': 'FALLIDA'}

    for fname in data_payloads:
        fpath = os.path.join(input_path, fname)
        try:
            if fname.endswith('.xlsx'):
                df = pl.from_pandas(pd.read_excel(fpath))
            else:
                df = pl.read_csv(fpath, separator=';', infer_schema_length=0, ignore_errors=True)

            df.columns = [c.strip() for c in df.columns]
            cols = df.columns
            res = None

            if 'SMS' in cols and 'Dato_Contacto' in cols:
                conditional = "SMS Saem"
                date_match = re.search(r'(\d{8})_(\d{4})', fname)
                dt_str = datetime.strptime(date_match.group(0), '%d%m%Y_%H%M').strftime('%Y-%m-%dT%H:%M:%S') if date_match else None
                
                res = df.select([
                    pl.col('SMS').alias('gestion'),
                    pl.lit("87910__anthony.quiva239").alias('usuario'),
                    pl.lit(dt_str).alias('fechagestion'),
                    pl.lit("Envio manual Syncra").alias('accion'),
                    pl.lit("MENSAJERIA SAEM").alias('perfil'),
                    pl.col('Dato_Contacto').alias('demografico'),
                    pl.col('Identificacion').alias('identificacion'),
                    (pl.col("Cuenta_Next").cast(pl.Utf8) + "-").alias('cuenta_promesa'),
                    pl.lit("claro").alias('campana')
                ])

            elif mapping_df is not None:
                id_col = next((c for c in ['IDENTIFICACION', 'Identificacion', 'Identificación'] if c in cols), None)
                if id_col:
                    df = df.with_columns(pl.col(id_col).cast(pl.Utf8).str.strip_chars())
                    joined = df.join(mapping_df, left_on=id_col, right_on='Cuenta_Next', how='inner')
                    
                    if not joined.is_empty():
                        if 'ESTADO' in cols:
                            conditional = "Blaster"
                            res = joined.select([
                                (pl.col('ESTADO').replace(status_map) + " - " + pl.col('NOMBRE DE LA CAMPAÑA').fill_null("BLASTER") + " - Duracion: " + pl.col('DURACION')).alias('gestion'),
                                pl.lit("Caller ID rotativo").alias('usuario'),
                                pl.col('FECHA DE MARCACION').alias('fechagestion'),
                                pl.lit("Ejecucion del Blaster").alias('accion'),
                                pl.lit("BLASTER CONTROLNEXT").alias('perfil'),
                                pl.col('NUMERO MARCADO').alias('demografico'),
                                pl.col('Documento').alias('identificacion'),
                                (pl.col(id_col).cast(pl.Utf8) + "-").alias('cuenta_promesa'),
                                pl.lit("claro").alias('campana')
                            ])
                        elif 'Mejor_Marcacion' in cols:
                            conditional = "IVR Saem"
                            res = joined.select([
                                (pl.col('Mejor_Marcacion').cast(pl.Utf8) + " - Duracion: " + pl.col('secounds').cast(pl.Utf8)).alias('gestion'),
                                pl.lit("Caller ID rotativo").alias('usuario'),
                                pl.col('Fecha_Inicio_Ultima_Llamada').alias('fechagestion'),
                                pl.lit("Envio manual Syncra").alias('accion'),
                                pl.lit("IVR SAEM").alias('perfil'),
                                pl.col('Celular').cast(pl.Utf8).str.slice(-10).alias('demografico'),
                                pl.col('Documento').alias('identificacion'),
                                (pl.col(id_col).cast(pl.Utf8) + "-").alias('cuenta_promesa'),
                                pl.lit("claro").alias('campana')
                            ])

            if res is not None:
                res = res.select([pl.all().cast(pl.Utf8)])
                final_dfs.append(res)
                print(f"✅ {fname} procesado en {conditional}")
        except Exception as e:
            print(f"❌ Error en {fname}: {e}")

    if final_dfs:
        output_df = pl.concat(final_dfs)
        output_df = output_df.drop_nulls(subset=['fechagestion', 'demografico', 'identificacion', 'cuenta_promesa']).unique()
        
        print("\n📊 --- RESUMEN DE REGISTROS POR PERFIL ---")
        conteo = output_df.group_by("perfil").len(name="cantidad")
        print(conteo)
        print(f"Total registros finales: {output_df.height:,}")

        out_file = os.path.join(output_path, f"batch_api_claro_{datetime.now().strftime('%Y%m%d_%H%M')}.csv")
        output_df.write_csv(out_file, separator=';')
        return f"🚀 ¡Hecho! Archivo guardado en: {out_file}"
    
    return "∅ No se procesó nada."