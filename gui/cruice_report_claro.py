import polars as pl
import os
import re
from pathlib import Path
from datetime import datetime

def report_claro_masive(input_folder: str, output_folder: str) -> str:
    print(f"🔍 Escaneando carpeta: {input_folder}")
    
    Path(output_folder).mkdir(parents=True, exist_ok=True)
    
    files = [f for f in os.listdir(input_folder) if f.lower().endswith('.csv')]
    
    if not files:
        return "❌ No se encontraron archivos CSV"
    
    print(f"📁 Encontrados {len(files)} archivos")
    
    df_next = None
    df_promesa = None
    
    for file in files:
        file_path = os.path.join(input_folder, file)
        
        try:
            df = pl.read_csv(
                file_path,
                separator=';',
                encoding='utf8',
                try_parse_dates=False,
                infer_schema_length=0
            )
            
            cols = [c.lower().strip() for c in df.columns]
            
            if 'cuenta_next' in cols:
                df_next = df
            elif 'cuenta_promesa' in cols:
                df_promesa = df
        
        except Exception as e:
            print(f"⚠️  Error en {file}: {str(e)[:50]}")
            continue
    
    if df_next is None or df_promesa is None:
        return "❌ No se encontraron los archivos necesarios"
    
    print("🔄 Procesando cruce...")
    
    cuenta_col = None
    for col in df_next.columns:
        if col.lower() == 'cuenta':
            cuenta_col = col
            break
    
    if cuenta_col is None:
        return "❌ No se encontró la columna 'cuenta' en el archivo de cruce"
    
    cuenta_next_col = None
    for col in df_next.columns:
        if col.lower() == 'cuenta_next':
            cuenta_next_col = col
            break
    
    if cuenta_next_col is None:
        return "❌ No se encontró la columna 'cuenta_next' en el archivo de cruce"
    
    df_next = df_next.with_columns([
        pl.col(cuenta_col).str.replace_all('-', '').alias('cuenta_clean'),
        pl.col(cuenta_next_col).str.replace_all('-', '').alias('cuenta_next_clean')
    ])
    
    cuenta_promesa_col = None
    for col in df_promesa.columns:
        if col.lower() == 'cuenta_promesa':
            cuenta_promesa_col = col
            break
    
    if cuenta_promesa_col is None:
        return "❌ No se encontró la columna 'cuenta_promesa' en el archivo base"
    
    df_promesa = df_promesa.with_columns([
        pl.col(cuenta_promesa_col).str.replace_all('-', '').alias('cuenta_promesa_clean')
    ])
    
    df_merged = df_promesa.join(
        df_next,
        left_on='cuenta_promesa_clean',
        right_on='cuenta_next_clean',
        how='inner'
    )
    
    print(f"📊 Registros: {df_merged.height:,}")
    
    def extract_duration(gestion_text):
        if gestion_text is None:
            return "00:00:00"
        
        text = str(gestion_text)
        
        match = re.search(r'Duracion:\s*(\d{2}:\d{2}:\d{2})', text)
        if match:
            return match.group(1)
        
        match_num = re.search(r'Duracion:\s*(\d+)', text)
        if match_num:
            secs = int(match_num.group(1))
            hours = secs // 3600
            minutes = (secs % 3600) // 60
            seconds = secs % 60
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        
        match_any = re.search(r'(\d+)\s*$', text)
        if match_any:
            secs = int(match_any.group(1))
            hours = secs // 3600
            minutes = (secs % 3600) // 60
            seconds = secs % 60
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        
        return "00:00:00"
    
    def extract_tipificacion(gestion_text):
        if gestion_text is None:
            return ""
        parts = str(gestion_text).split(' - ')
        return parts[0].strip() if parts else ""
    
    col_map = {}
    for target_col, possible_names in [
        ('nombre_del_cliente', ['nombre_del_cliente', 'nombre', 'cliente']),
        ('marca_asignada', ['marca_asignada', 'marca_asignada']),
        ('fecha_ingreso', ['fecha_ingreso', 'fecha_ingreso']),
        ('valor_deuda', ['valor_deuda', 'deuda', 'valor']),
        ('crm_origen', ['crm_origen', 'crm']),
        ('segmento_camunif', ['segmento_camunif', 'segmento']),
        ('perfil', ['perfil', 'perfil']),
        ('demografico', ['demografico', 'demografico']),
        ('fechagestion', ['fechagestion', 'fecha_gestion']),
        ('gestion', ['gestion', 'gestion'])
    ]:
        found = False
        for col in df_merged.columns:
            if col.lower() == possible_names[0]:
                col_map[target_col] = col
                found = True
                break
        if not found and len(possible_names) > 1:
            for col in df_merged.columns:
                if possible_names[1] in col.lower():
                    col_map[target_col] = col
                    found = True
                    break
    
    required_cols = ['nombre_del_cliente', 'marca_asignada', 'valor_deuda', 'crm_origen', 
                     'segmento_camunif', 'perfil', 'demografico', 'fechagestion', 'gestion']
    missing = [col for col in required_cols if col not in col_map]
    if missing:
        return f"❌ Faltan columnas: {missing}"
    
    df_final = df_merged.with_columns([
        pl.lit("13").alias("id_casa_cobranza"),
        (pl.col(cuenta_col)).alias("Cuenta"),
        pl.col(col_map['nombre_del_cliente']).alias("nombre_completo"),
        pl.col(col_map['marca_asignada']).alias("edad_mora_asignada"),
        pl.col(col_map['fechagestion']).str.to_datetime(format=None, strict=False).dt.strftime("%H:%M:%S").alias("hora_gestion"),
        pl.col(col_map['fechagestion']).str.to_datetime(format=None, strict=False).dt.date().alias("fecha_gestion"),
        pl.col(col_map['gestion']).map_elements(extract_duration, return_dtype=pl.Utf8).alias("duracion_gestion"),
        pl.col(col_map['perfil']).alias("nombre_asesor"),
        pl.col(col_map.get('fecha_ingreso', '')).alias("fecha_asignacion"),
        pl.col(col_map['gestion']).map_elements(extract_tipificacion, return_dtype=pl.Utf8).alias("tipificacion"),
        pl.lit("").alias("motivo_no_pago"),
        pl.lit("VIRTUAL").alias("canal"),
        pl.col(col_map['valor_deuda']).alias("monto_asignado"),
        pl.col(col_map['crm_origen']).alias("crm"),
        pl.col(col_map['segmento_camunif']).alias("segmento"),
        pl.lit("NO").alias("contactado"),
        pl.col(col_map['demografico']).alias("linea_telefonica_mail"),
        pl.lit("").alias("fecha_realizacion_promesa"),
        pl.lit("").alias("fecha_compromiso_pago")
    ]).select([
        "id_casa_cobranza", "Cuenta", "nombre_completo", "edad_mora_asignada",
        "hora_gestion", "fecha_gestion", "duracion_gestion", "nombre_asesor",
        "fecha_asignacion", "tipificacion", "motivo_no_pago", "canal",
        "monto_asignado", "crm", "segmento", "contactado",
        "linea_telefonica_mail", "fecha_realizacion_promesa", "fecha_compromiso_pago"
    ])
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    output_completo = os.path.join(output_folder, f"reporte_cobro_completo_{timestamp}.csv")
    df_final.write_csv(output_completo, separator=';')
    
    df_efectivo = df_final.filter(
        pl.col("tipificacion").str.to_lowercase().str.contains("contestada|satisfactorio")
    )
    output_efectivo = os.path.join(output_folder, f"reporte_efectivo_blasters_{timestamp}.csv")
    df_efectivo.write_csv(output_efectivo, separator=';')

    df_no_efectivo = df_final.filter(
        ~pl.col("tipificacion").str.to_lowercase().str.contains("contestada|satisfactorio")
    )
    df_no_efectivo = df_no_efectivo.filter(
        ~pl.col("nombre_asesor").str.to_lowercase().str.contains("mensajer")
    )
    output_no_efectivo = os.path.join(output_folder, f"reporte_no_efectivo_blasters_{timestamp}.csv")
    df_no_efectivo.write_csv(output_no_efectivo, separator=';')
    
    df_mensajes = df_final.filter(
        pl.col("nombre_asesor").str.to_lowercase().str.contains("mensajer")
    )
    output_mensajes = os.path.join(output_folder, f"reporte_mensajes_{timestamp}.csv")
    df_mensajes.write_csv(output_mensajes, separator=';')
    
    print(f"✅ Procesado: {df_final.height:,} registros totales")
    print(f"   🔹 Contestadas/Satisfactorio: {df_efectivo.height:,}")
    print(f"   🔹 No Contestadas/No Satisfactorio: {df_no_efectivo.height:,}")
    print(f"   🔹 Mensajería: {df_mensajes.height:,}")
    print(f"💾 Archivos guardados:")
    print(f"   📄 Completo: {output_completo}")
    print(f"   📄 Blasters: {output_efectivo}")
    print(f"   📄 No Efectivo: {output_no_efectivo}")
    print(f"   📄 Mensajes: {output_mensajes}")
    
    return f"✅ Archivos guardados en: {output_folder}"