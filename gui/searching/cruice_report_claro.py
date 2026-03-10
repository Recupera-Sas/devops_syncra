import polars as pl
import os
import re
from pathlib import Path
from datetime import datetime

def report_claro_masive(input_folder: str, output_folder: str) -> str:
    print(f"🔍 Escaneando carpeta: {input_folder}")
    
    Path(output_folder).mkdir(parents=True, exist_ok=True)
    
    all_files = os.listdir(input_folder)
    csv_files = [f for f in all_files if f.lower().endswith('.csv')]
    parquet_files = [f for f in all_files if f.lower().endswith(('.parquet', '.pq'))]
    files = csv_files + parquet_files
    
    if not files:
        return "❌ No se encontraron archivos CSV o Parquet"
    
    print(f"📁 Encontrados {len(files)} archivos")
    
    df_next = None
    df_promesa = None
    
    for file in files:
        file_path = os.path.join(input_folder, file)
        
        try:
            if file.lower().endswith('.csv'):
                df = pl.read_csv(
                    file_path,
                    separator=';',
                    encoding='utf8',
                    try_parse_dates=False,
                    infer_schema_length=0
                )
            else:
                df = pl.read_parquet(file_path)
            
            cols = [c.lower().strip() for c in df.columns]
            
            if 'cuenta_next' in cols:
                df_next = df
                print(f"\n📄 Archivo NEXT: {file}")
                print(f"   Registros: {df.height:,}")
            elif 'cuenta_promesa' in cols:
                df_promesa = df
                print(f"\n📄 Archivo PROMESA: {file}")
                print(f"   Registros: {df.height:,}")
                # Mostrar distribución de perfiles
                if 'perfil' in df.columns:
                    perfil_counts = df.group_by('perfil').agg(pl.len().alias('count')).sort('count', descending=True)
                    print("   Distribución de perfiles:")
                    for row in perfil_counts.rows():
                        print(f"      {row[0]}: {row[1]:,}")
        
        except Exception as e:
            print(f"⚠️  Error en {file}: {str(e)[:50]}")
            continue
    
    if df_next is None or df_promesa is None:
        return "❌ No se encontraron los archivos necesarios"
    
    print("\n🔄 Procesando cruce por etapas...")
    
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
    
    cuenta_promesa_col = None
    for col in df_promesa.columns:
        if col.lower() == 'cuenta_promesa':
            cuenta_promesa_col = col
            break
    
    if cuenta_promesa_col is None:
        return "❌ No se encontró la columna 'cuenta_promesa' en el archivo base"
    
    # PASO 1: Limpieza básica en NEXT
    print("\n🧹 PASO 1: Limpieza básica de cuentas...")
    df_next = df_next.with_columns([
        pl.col(cuenta_col).alias('cuenta_original'),
        pl.col(cuenta_next_col)
        .str.replace_all(r'[.\-]', '')  # Eliminar puntos y guiones
        .str.replace(r'^0+', '')         # Eliminar ceros a la izquierda
        .alias('cuenta_next_clean_1')
    ])
    
    # PASO 2: Limpieza básica en PROMESA
    df_promesa = df_promesa.with_columns([
        pl.col(cuenta_promesa_col)
        .str.replace_all(r'[.\-]', '')  # Eliminar puntos y guiones
        .alias('cuenta_promesa_clean_1')
    ])
    
    # PASO 3: Crear versiones alternativas para PROMESA
    print("\n🔧 PASO 2: Creando versiones alternativas de cuentas para PROMESA...")
    
    # CORRECCIÓN: Manejar caso cuando la cadena está vacía
    df_promesa = df_promesa.with_columns([
        # Versión sin ceros a la izquierda
        pl.col('cuenta_promesa_clean_1')
        .str.replace(r'^0+', '')
        .alias('cuenta_promesa_sin_ceros'),
        
        # Versión quitando el último dígito (solo si tiene más de 1 dígito)
        pl.when(
            pl.col('cuenta_promesa_clean_1').str.len_chars() > 1
        )
        .then(
            pl.col('cuenta_promesa_clean_1').str.slice(0, pl.col('cuenta_promesa_clean_1').str.len_chars() - 1)
        )
        .otherwise(pl.col('cuenta_promesa_clean_1'))
        .alias('cuenta_promesa_sin_ultimo_digito'),
        
        # Versión con ceros a la izquierda (para casos como 1235551001010 que debería ser 01235551001010)
        pl.when(
            pl.col('cuenta_promesa_clean_1').str.len_chars() == 13  # Si tiene 13 dígitos
        )
        .then(
            pl.lit("0") + pl.col('cuenta_promesa_clean_1')  # Agregar cero al inicio
        )
        .otherwise(pl.col('cuenta_promesa_clean_1'))
        .alias('cuenta_promesa_con_cero_inicial')
    ])
    
    # PASO 4: Crear versiones alternativas para NEXT
    print("🔧 PASO 3: Creando versiones alternativas de cuentas para NEXT...")
    df_next = df_next.with_columns([
        # Versión con ceros a la izquierda (para casos donde NEXT tiene el cero)
        pl.when(
            pl.col('cuenta_next_clean_1').str.len_chars() == 13  # Si tiene 13 dígitos
        )
        .then(
            pl.lit("0") + pl.col('cuenta_next_clean_1')  # Agregar cero al inicio
        )
        .otherwise(pl.col('cuenta_next_clean_1'))
        .alias('cuenta_next_con_cero_inicial')
    ])
    
    # Mostrar ejemplos de las transformaciones
    print("\n🔍 Ejemplos de transformaciones en PROMESA:")
    ejemplos_promesa = df_promesa.select([
        pl.col(cuenta_promesa_col).alias('original'),
        'cuenta_promesa_clean_1',
        'cuenta_promesa_sin_ceros',
        'cuenta_promesa_sin_ultimo_digito',
        'cuenta_promesa_con_cero_inicial'
    ]).head(10)
    print(ejemplos_promesa)
    
    print("\n🔍 Ejemplos de transformaciones en NEXT:")
    ejemplos_next = df_next.select([
        pl.col(cuenta_next_col).alias('original'),
        'cuenta_next_clean_1',
        'cuenta_next_con_cero_inicial'
    ]).head(10)
    print(ejemplos_next)
    
    # PASO 5: Realizar cruces por etapas
    print("\n🔄 PASO 4: Realizando cruces por etapas...")
    
    # Etapa 1: Cruce básico (limpieza estándar)
    print("   Etapa 1: Cruce con limpieza básica...")
    df_merged_1 = df_promesa.join(
        df_next,
        left_on='cuenta_promesa_clean_1',
        right_on='cuenta_next_clean_1',
        how='inner'
    )
    print(f"      Registros en etapa 1: {df_merged_1.height:,}")
    
    # Identificar cuentas de PROMESA que ya cruzaron
    cuentas_cruzadas = set(df_merged_1['cuenta_promesa_clean_1'].to_list()) if df_merged_1.height > 0 else set()
    
    # Etapa 2: Cruce con PROMESA sin ceros vs NEXT limpio
    print("   Etapa 2: Cruce con PROMESA sin ceros vs NEXT limpio...")
    df_promesa_restante = df_promesa.filter(~pl.col('cuenta_promesa_clean_1').is_in(cuentas_cruzadas))
    
    df_merged_2 = df_promesa_restante.join(
        df_next,
        left_on='cuenta_promesa_sin_ceros',
        right_on='cuenta_next_clean_1',
        how='inner'
    )
    print(f"      Registros en etapa 2: {df_merged_2.height:,}")
    
    # Actualizar cuentas cruzadas
    nuevas_cuentas = set(df_merged_2['cuenta_promesa_clean_1'].to_list()) if df_merged_2.height > 0 else set()
    cuentas_cruzadas.update(nuevas_cuentas)
    
    # Etapa 3: Cruce con PROMESA sin último dígito vs NEXT limpio
    print("   Etapa 3: Cruce quitando último dígito de PROMESA...")
    df_promesa_restante = df_promesa.filter(~pl.col('cuenta_promesa_clean_1').is_in(cuentas_cruzadas))
    
    df_merged_3 = df_promesa_restante.join(
        df_next,
        left_on='cuenta_promesa_sin_ultimo_digito',
        right_on='cuenta_next_clean_1',
        how='inner'
    )
    print(f"      Registros en etapa 3: {df_merged_3.height:,}")
    
    # Actualizar cuentas cruzadas
    nuevas_cuentas = set(df_merged_3['cuenta_promesa_clean_1'].to_list()) if df_merged_3.height > 0 else set()
    cuentas_cruzadas.update(nuevas_cuentas)
    
    # Etapa 4: Cruce con PROMESA con cero inicial vs NEXT con cero inicial
    print("   Etapa 4: Cruce con cero inicial en ambas...")
    df_promesa_restante = df_promesa.filter(~pl.col('cuenta_promesa_clean_1').is_in(cuentas_cruzadas))
    
    df_merged_4 = df_promesa_restante.join(
        df_next,
        left_on='cuenta_promesa_con_cero_inicial',
        right_on='cuenta_next_con_cero_inicial',
        how='inner'
    )
    print(f"      Registros en etapa 4: {df_merged_4.height:,}")
    
    # Combinar todos los resultados
    print("\n📊 Combinando resultados de todas las etapas...")
    dfs_to_concat = []
    if df_merged_1.height > 0:
        dfs_to_concat.append(df_merged_1)
    if df_merged_2.height > 0:
        dfs_to_concat.append(df_merged_2)
    if df_merged_3.height > 0:
        dfs_to_concat.append(df_merged_3)
    if df_merged_4.height > 0:
        dfs_to_concat.append(df_merged_4)
    
    if dfs_to_concat:
        df_merged = pl.concat(dfs_to_concat)
        # Eliminar duplicados por si alguna cuenta cruzó en múltiples etapas
        df_merged = df_merged.unique(subset=['cuenta_promesa_clean_1'])
    else:
        df_merged = df_promesa.clear()  # DataFrame vacío
    
    print(f"\n📊 Registros totales después de todas las etapas: {df_merged.height:,}")
    
    # Identificar registros que NO CRUZARON en ninguna etapa
    cuentas_finales_cruzadas = set(df_merged['cuenta_promesa_clean_1'].to_list()) if df_merged.height > 0 else set()
    df_promesa_sin_match = df_promesa.filter(~pl.col('cuenta_promesa_clean_1').is_in(cuentas_finales_cruzadas))
    
    print(f"\n📊 Registros que NO CRUZARON en ninguna etapa: {df_promesa_sin_match.height:,}")
    
    # Guardar registros que no cruzaron
    timestamp_analisis = datetime.now().strftime('%Y%m%d_%H%M%S')
    if df_promesa_sin_match.height > 0:
        output_sin_match = os.path.join(output_folder, f"registros_sin_match_promesa_{timestamp_analisis}.csv")
        
        print("\n📊 Distribución de perfiles en registros SIN MATCH:")
        sin_match_perfiles = df_promesa_sin_match.group_by('perfil').agg(pl.len().alias('count')).sort('count', descending=True)
        for row in sin_match_perfiles.rows():
            print(f"      {row[0]}: {row[1]:,}")
        
        df_promesa_sin_match.write_csv(output_sin_match, separator=';')
        print(f"   ✅ Guardados en: {output_sin_match}")
    
    # Verificar correos después del cruce
    if 'perfil' in df_merged.columns:
        correos_despues = df_merged.filter(
            pl.col('perfil').str.to_lowercase().str.contains("correo|corre")
        ).height
        print(f"\n📧 CORREOS después del cruce por etapas: {correos_despues:,}")
    
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
    
    if 'Debt_Age_Inicial' in df_next.columns:
        df_merged = df_merged.with_columns([
            pl.when(pl.col(col_map['marca_asignada']) == "120 - 180")
            .then(pl.col('Debt_Age_Inicial'))
            .otherwise(pl.col(col_map['marca_asignada']))
            .alias('marca_asignada_corregida')
        ])
        marca_col = 'marca_asignada_corregida'
    else:
        marca_col = col_map['marca_asignada']
    
    df_final = df_merged.with_columns([
        pl.lit("13").alias("id_casa_cobranza"),
        pl.col('cuenta_original').alias("Cuenta"),
        pl.col(col_map['nombre_del_cliente']).alias("nombre_completo"),
        pl.col(marca_col).alias("edad_mora_asignada"),
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
    
    # Guardar archivo con todos los registros de CORREO (filtro simple)
    df_correo_total = df_final.filter(
        pl.col("nombre_asesor").str.to_lowercase().str.contains("corr")
    )
    print(f"\n📧 TOTAL REGISTROS CON 'CORR' después de cruce por etapas: {df_correo_total.height:,}")
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    output_completo = os.path.join(output_folder, f"gestion_final_completo_{timestamp}.csv")
    df_final.write_csv(output_completo, separator=';')
    
    # Guardar archivo con TODOS los correos
    output_correo_total = os.path.join(output_folder, f"todos_los_correos_{timestamp}.csv")
    df_correo_total.write_csv(output_correo_total, separator=';')
    
    df_efectivo = df_final.filter(
        pl.col("tipificacion").str.to_lowercase().str.contains("contestada|satisfactorio")
    )
    output_efectivo = os.path.join(output_folder, f"reporte_efectivo_blasters_{timestamp}.csv")
    df_efectivo.write_csv(output_efectivo, separator=';')
    
    df_no_efectivo = df_final.filter(
        ~pl.col("tipificacion").str.to_lowercase().str.contains("contestada|satisfactorio")
    )
    df_no_efectivo = df_no_efectivo.filter(
        pl.col("nombre_asesor").str.to_lowercase().str.contains("blaster|ivr")
    )
    output_no_efectivo = os.path.join(output_folder, f"reporte_no_efectivo_blasters_{timestamp}.csv")
    df_no_efectivo.write_csv(output_no_efectivo, separator=';')
    
    df_blasters = df_final.filter(
        pl.col("nombre_asesor").str.to_lowercase().str.contains("blaster|ivr")
    )
    output_blasters = os.path.join(output_folder, f"gestion_blasters_{timestamp}.csv")
    df_blasters.write_csv(output_blasters, separator=';')
    
    df_mensajes = df_final.filter(
        pl.col("nombre_asesor").str.to_lowercase().str.contains("mensajer")
    )
    output_mensajes = os.path.join(output_folder, f"reporte_mensajes_{timestamp}.csv")
    df_mensajes.write_csv(output_mensajes, separator=';')
    
    # CORREOS - filtrado simple y procesamiento
    df_correo = df_final.filter(
        pl.col("nombre_asesor").str.to_lowercase().str.contains("corr")
    )
    
    if df_correo.height > 0:
        print(f"\n📧 Procesando {df_correo.height:,} registros de correo")
        
        # Función para extraer el email de la tipificación
        def extract_email_from_tipificacion(row):
            tipificacion = row.get('tipificacion', '')
            if tipificacion and '|' in str(tipificacion):
                parts = str(tipificacion).split('|')
                return parts[-1].strip()
            return row.get('linea_telefonica_mail', '')
        
        # Aplicar la extracción de email
        df_correo = df_correo.with_columns([
            pl.struct(['tipificacion', 'linea_telefonica_mail'])
            .map_elements(extract_email_from_tipificacion, return_dtype=pl.Utf8)
            .alias('linea_telefonica_mail_corregido')
        ])
        
        # Actualizar la columna con el email extraído
        df_correo = df_correo.with_columns([
            pl.when(pl.col('linea_telefonica_mail_corregido') != '')
            .then(pl.col('linea_telefonica_mail_corregido'))
            .otherwise(pl.col('linea_telefonica_mail'))
            .alias('linea_telefonica_mail')
        ]).drop('linea_telefonica_mail_corregido')
        
        # También actualizar la tipificación para quitar el email
        def clean_tipificacion(tipificacion):
            if tipificacion and '|' in str(tipificacion):
                parts = str(tipificacion).split('|')
                return '|'.join(parts[:-1])
            return tipificacion
        
        df_correo = df_correo.with_columns([
            pl.col('tipificacion')
            .map_elements(clean_tipificacion, return_dtype=pl.Utf8)
            .alias('tipificacion')
        ])
    
    output_correo = os.path.join(output_folder, f"reporte_correos_{timestamp}.csv")
    df_correo.write_csv(output_correo, separator=';')
    
    print(f"\n✅ RESUMEN FINAL:")
    print(f"   📊 Total registros: {df_final.height:,}")
    print(f"   🔹 Contestadas/Satisfactorio: {df_efectivo.height:,}")
    print(f"   🔹 Blasters+IVR: {df_blasters.height:,}")
    print(f"   🔹 Mensajería: {df_mensajes.height:,}")
    print(f"   🔹 CORREOS (todos los que contienen 'corr'): {df_correo.height:,}")
    
    return procesar_archivo_final(output_folder, output_folder)

def procesar_archivo_final(input_folder: str, output_folder: str) -> str:
    print("🔄 Procesando archivo final con llave y cruce por prioridad...")
    
    csv_files = [f for f in os.listdir(input_folder) if f.lower().endswith('.csv')]
    parquet_files = [f for f in os.listdir(input_folder) if f.lower().endswith(('.parquet', '.pq'))]
    all_files = csv_files + parquet_files
    
    archivo_final = None
    for file in all_files:
        if 'completo' in file.lower():
            archivo_final = file
            break
    
    if archivo_final is None:
        return "❌ No se encontró archivo completo para procesar"
    
    file_path = os.path.join(input_folder, archivo_final)
    
    if archivo_final.lower().endswith('.csv'):
        df = pl.read_csv(
            file_path, 
            separator=';', 
            try_parse_dates=False,
            infer_schema_length=10000,
            encoding='utf8',
            ignore_errors=True
        )
    else:
        df = pl.read_parquet(file_path)
    
    for col in df.columns:
        if df[col].dtype in [pl.Int64, pl.Float64] and col in ['edad_mora_asignada', 'tipificacion', 'nombre_asesor']:
            df = df.with_columns(pl.col(col).cast(pl.String))
    
    if 'linea_telefonica_mail' not in df.columns or 'Cuenta' not in df.columns:
        df_columns_lower = {c.lower(): c for c in df.columns}
        if 'linea_telefonica_mail' in df_columns_lower:
            df = df.rename({df_columns_lower['linea_telefonica_mail']: 'linea_telefonica_mail'})
        if 'cuenta' in df_columns_lower:
            df = df.rename({df_columns_lower['cuenta']: 'Cuenta'})
    
    if 'linea_telefonica_mail' not in df.columns:
        df = df.with_columns(pl.lit('').alias('linea_telefonica_mail'))
    if 'Cuenta' not in df.columns:
        df = df.with_columns(pl.lit('').alias('Cuenta'))
    
    df = df.with_columns([
        pl.col('linea_telefonica_mail').cast(pl.String).fill_null('').alias('linea_telefonica_mail_str'),
        pl.col('Cuenta').cast(pl.String).fill_null('').alias('Cuenta_str')
    ])
    
    df = df.with_columns([
        (pl.col('linea_telefonica_mail_str') + '_' + pl.col('Cuenta_str')).alias('llave')
    ])
    
    if 'canal' not in df.columns:
        df = df.with_columns(pl.lit('').alias('canal'))
    
    df_agente = df.filter(pl.col('canal') == 'AGENTE')
    
    otros_dfs = []
    
    for col in ['tipificacion', 'nombre_asesor']:
        if col not in df.columns:
            df = df.with_columns(pl.lit('').alias(col))
    
    df_contestadas = df.filter(
        (pl.col('canal') != 'AGENTE') & 
        (pl.col('tipificacion').str.to_lowercase().str.starts_with('contestada'))
    )
    if df_contestadas.height > 0:
        otros_dfs.append(df_contestadas)
    
    df_blaster = df.filter(
        (pl.col('canal') != 'AGENTE') & 
        (~pl.col('tipificacion').str.to_lowercase().str.starts_with('contestada')) &
        (pl.col('nombre_asesor').str.to_lowercase().str.contains('blaster|ivr'))
    )
    if df_blaster.height > 0:
        otros_dfs.append(df_blaster)
    
    df_mensajeria = df.filter(
        (pl.col('canal') != 'AGENTE') & 
        (~pl.col('tipificacion').str.to_lowercase().str.starts_with('contestada')) &
        (~pl.col('nombre_asesor').str.to_lowercase().str.contains('blaster|ivr')) &
        (pl.col('nombre_asesor').str.to_lowercase().str.contains('mensajer'))
    )
    if df_mensajeria.height > 0:
        otros_dfs.append(df_mensajeria)
    
    # CORREOS - filtrado simple por "corr"
    df_correo = df.filter(
        (pl.col('canal') != 'AGENTE') & 
        (~pl.col('tipificacion').str.to_lowercase().str.starts_with('contestada')) &
        (pl.col('nombre_asesor').str.to_lowercase().str.contains("corr"))
    )
    if df_correo.height > 0:
        otros_dfs.append(df_correo)
    
    df_resto = df.filter(
        (pl.col('canal') != 'AGENTE') & 
        (~pl.col('tipificacion').str.to_lowercase().str.starts_with('contestada')) &
        (~pl.col('nombre_asesor').str.to_lowercase().str.contains('blaster|ivr')) &
        (~pl.col('nombre_asesor').str.to_lowercase().str.contains('mensajer')) &
        (~pl.col('nombre_asesor').str.to_lowercase().str.contains("corr"))
    )
    if df_resto.height > 0:
        otros_dfs.append(df_resto)
    
    llaves_procesadas = set(df_agente['llave'].to_list()) if df_agente.height > 0 else set()
    dfs_a_combinar = [df_agente] if df_agente.height > 0 else []
    
    for otro_df in otros_dfs:
        df_filtrado = otro_df.filter(~pl.col('llave').is_in(llaves_procesadas))
        if df_filtrado.height > 0:
            dfs_a_combinar.append(df_filtrado)
            nuevas_llaves = set(df_filtrado['llave'].to_list())
            llaves_procesadas.update(nuevas_llaves)
    
    if not dfs_a_combinar:
        df_final_combinado = df
    else:
        df_final_combinado = pl.concat(dfs_a_combinar)
    
    df_final_combinado = df_final_combinado.unique(subset=['llave'], keep='first')
    
    monitor_keywords = ['blaster', 'ivr', 'mensajer', 'correo', 'corre']
    monitor_expr = pl.when(
        pl.col('nombre_asesor').str.to_lowercase().str.contains_any(monitor_keywords)
    ).then(
        pl.lit('BM')
    ).otherwise(
        pl.lit('')
    )
    
    df_final_combinado = df_final_combinado.with_columns([
        monitor_expr.alias('monitor')
    ])
    
    columnas_esperadas = [
        'id_casa_cobranza', 'Cuenta', 'nombre_completo', 'edad_mora_asignada',
        'hora_gestion', 'fecha_gestion', 'duracion_gestion', 'nombre_asesor',
        'fecha_asignacion', 'tipificacion', 'motivo_no_pago', 'canal',
        'monto_asignado', 'crm', 'segmento', 'contactado',
        'linea_telefonica_mail', 'fecha_realizacion_promesa', 'fecha_compromiso_pago',
        'llave', 'monitor'
    ]
    
    for col in columnas_esperadas:
        if col not in df_final_combinado.columns:
            df_final_combinado = df_final_combinado.with_columns(pl.lit('').alias(col))
    
    df_final_combinado = df_final_combinado.select(columnas_esperadas)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = os.path.join(output_folder, f"reporte_unificado_con_llave_{timestamp}.csv")
    
    df_final_combinado.write_csv(output_file, separator=';')
    
    print(f"\n✅ Archivo unificado generado: {output_file}")
    print(f"   📊 Total registros: {df_final_combinado.height:,}")
    
    return f"✅ Archivo unificado guardado en: {output_file}"