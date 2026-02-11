import polars as pl
from pathlib import Path
from datetime import datetime
import sys
import traceback

COLUMNS_RETIRADAS = {
    'EMPRESA': 'EMPRESA',
    'ID DE LA CAMPAÑA': 'ID DE LA CAMPAÑA',
    'NOMBRE DE LA CAMPAÑA': 'NOMBRE DE LA CAMPAÑA',
    'LEAD ID': 'LEAD ID',
    'identificacion': 'cuenta_next',
    'NUMERO MARCADO': 'NUMERO MARCADO',
    'ESTADO': 'ESTADO',
    'FECHA DE MARCACION': 'FECHA DE MARCACION',
    'DURACION': 'DURACION',
    'CRM': 'CRM',
    'MARCA': 'MARCA',
    'IDENTI': 'IDENTI',
    'TELEFONO 1': 'TELEFONO 1'
}

COLUMNAS_EFECTIVAS_BLASTER = {
    'CANAL': 'CANAL',
    'ESTADO': 'ESTADO',
    'FECHA_INICIO_ULTIMA_LLAMADA': 'FECHA_INICIO_ULTIMA_LLAMADA',
    'DURACION_SEGUNDOS': 'DURACION_SEGUNDOS',
    'CELULAR': 'CELULAR',
    'IDENTIFICACION': 'IDENTIFICACION',
    'MARCA': 'MARCA',
    'crm_origen': 'crm_origen',
    'VALOR_SCORING_DUPLICATED_0': 'VALOR_SCORING_DUPLICATED_0',
    'MONITOR': 'MONITOR',
    'RANGO_SALDO': 'RANGO_SALDO',
    'NOMBRE_CAMPANA': 'NOMBRE_CAMPANA',
    'MEJORPERFIL_MES': 'MEJORPERFIL_MES',
    'NOMBRE DE LA CAMPAÑA': 'NOMBRE DE LA CAMPAÑA'
}

COLUMNAS_EFECTIVAS_SAEM = {
    'CANAL': 'CANAL',
    'ESTADO': 'ESTADO',
    'FECHA_INICIO_ULTIMA_LLAMADA': 'FECHA_INICIO_ULTIMA_LLAMADA',
    'DURACION_SEGUNDOS': 'DURACION_SEGUNDOS',
    'CELULAR': 'CELULAR',
    'IDENTIFICACION': 'IDENTIFICACION',
    'MARCA': 'MARCA',
    'crm_origen': 'crm_origen',
    'VALOR_SCORING_DUPLICATED_0': 'VALOR_SCORING_DUPLICATED_0',
    'MONITOR': 'MONITOR',
    'RANGO_SALDO': 'RANGO_SALDO',
    'NOMBRE_CAMPANA': 'NOMBRE_CAMPANA',
    'MEJORPERFIL_MES': 'MEJORPERFIL_MES',
    'NOMBRE DE LA CAMPAÑA': 'NOMBRE DE LA CAMPAÑA'
}

def normalize_column_names(df):
    print(f"  - Normalizando nombres de columnas. Columnas actuales: {df.columns}")
    column_mapping = {}
    for col in df.columns:
        normalized_col = col
        if "FECHA DE MARCACI" in col.upper():
            if "Ó" in col or "Ó" in col:
                normalized_col = "FECHA DE MARCACION"
            else:
                normalized_col = "FECHA DE MARCACION"
        elif "DURACI" in col.upper():
            if "Ó" in col or "Ó" in col:
                normalized_col = "DURACION"
            else:
                normalized_col = "DURACION"
        column_mapping[col] = normalized_col
    
    if column_mapping:
        df = df.rename(column_mapping)
        print(f"  - Columnas normalizadas: {df.columns}")
    
    return df

def translate_estado_blaster(value):
    translations = {
        'ANSWERED': 'CONTESTADA',
        'NO ANSWER': 'NO CONTESTA',
        'BUZON': 'BUZON DE VOZ',
        'BUSY': 'OCUPADO',
        'FAILED': 'FALLIDA'
    }
    return translations.get(value, value)

def translate_estado_ivr_saem(value):
    translations = {
        'ANSWERED': 'CONTESTADA',
        'NO ANSWER': 'NO CONTESTA',
        'BUZON': 'BUZON DE VOZ',
        'BUSY': 'OCUPADO',
        'FAILED': 'FALLIDA',
        'Satisfactorio': 'CONTESTADA',
        'Colgo': 'CONTESTADA',
        'No Contesta': 'NO CONTESTA',
        'Maquina': 'BUZON DE VOZ',
        'Congestion': 'OCUPADO',
        'Numero Telefonico Invalido': 'FALLIDA'
    }
    return translations.get(value, value)

def format_duration_to_seconds(duration_str):
    if duration_str is None:
        return 0
    
    try:
        if isinstance(duration_str, (int, float)):
            return int(duration_str)
        
        duration_str = str(duration_str).strip()
        
        if duration_str.replace('.', '', 1).isdigit():
            return int(float(duration_str))
        
        parts = duration_str.split(':')
        if len(parts) == 3:
            hours = int(parts[0]) if parts[0].isdigit() else 0
            minutes = int(parts[1]) if parts[1].isdigit() else 0
            seconds = int(float(parts[2])) if parts[2].replace('.', '', 1).isdigit() else 0
            return hours * 3600 + minutes * 60 + seconds
        elif len(parts) == 2:
            minutes = int(parts[0]) if parts[0].isdigit() else 0
            seconds = int(float(parts[1])) if parts[1].replace('.', '', 1).isdigit() else 0
            return minutes * 60 + seconds
        else:
            return 0
    except Exception as e:
        print(f"    - Error formateando duración '{duration_str}': {e}")
        return 0

def adjust_estado_for_duration(df):
    if 'ESTADO' not in df.columns or 'DURACION' not in df.columns:
        print(f"  - No se puede ajustar estado por duración: columnas ESTADO o DURACION no encontradas")
        return df

    print(f"  - Ajustando estados por duración...")
    df = df.with_columns(
        pl.col('DURACION')
        .map_elements(format_duration_to_seconds, return_dtype=pl.Int64)
        .alias('_duration_seconds')
    )
    
    sample_durations = df.select(['DURACION', '_duration_seconds']).head(5)
    print(f"  - Muestra de conversión de duración: {sample_durations}")
    
    df = df.with_columns(
        pl.when(
            (pl.col('ESTADO') == 'CONTESTADA') & 
            (pl.col('_duration_seconds') < 5)
        )
        .then(pl.lit('CONTESTADA PARCIAL'))
        .otherwise(pl.col('ESTADO'))
        .alias('ESTADO')
    )
    df = df.drop('_duration_seconds')
    return df

def process_celular_ivr_saem(df):
    if 'Celular' in df.columns:
        print(f"  - Procesando campo Celular para IVR SAEM")
        df = df.with_columns(
            pl.col('Celular')
            .cast(pl.Utf8)
            .str.replace_all('"', '')
            .str.slice(2)
            .alias('Celular')
        )
    return df

def normalize_ivr_saem_columns(df):
    print(f"  - Normalizando columnas IVR SAEM. Columnas actuales: {df.columns}")
    if 'Identificacion' in df.columns:
        df = df.rename({'Identificacion': 'identificacion'})
        df = df.with_columns(pl.col('identificacion').cast(pl.Utf8))
        print(f"  - Renombrado Identificacion a identificacion")
    elif 'IDENTIFICACION' in df.columns:
        df = df.rename({'IDENTIFICACION': 'identificacion'})
        df = df.with_columns(pl.col('identificacion').cast(pl.Utf8))
        print(f"  - Renombrado IDENTIFICACION a identificacion")
    df = process_celular_ivr_saem(df)
    if 'Mejor_Marcacion' in df.columns:
        print(f"  - Normalizando estados Mejor_Marcacion")
        df = df.with_columns(
            pl.col('Mejor_Marcacion')
            .map_elements(translate_estado_ivr_saem, return_dtype=pl.Utf8)
            .alias('MEJOR_MARCACION_NORMALIZADA')
        )
    return df

def get_estados_efectivos_saem():
    return ['Satisfactorio', 'Colgo']

def get_estados_efectivos_blaster():
    return ['CONTESTADA', 'COLGO']

def normalize_assignment_columns(df):
    print(f"  - Normalizando columnas de asignación")
    
    column_mapping = {}
    
    for col in df.columns:
        if col == 'Cuenta_Next':
            column_mapping[col] = 'cuenta_next'
        elif col == 'Liquidacion':
            column_mapping[col] = 'liquidacion'
        elif col == 'Monitor':
            column_mapping[col] = 'monitor'
        elif col == 'Valor Scoring':
            column_mapping[col] = 'valor_scoring'
        elif col == 'Marca_Asignada':
            column_mapping[col] = 'marca'
        elif col == 'CRM_Origen':
            column_mapping[col] = 'crm_origen'
        elif col == 'Rango_Deuda':
            column_mapping[col] = 'rango_saldo'
        elif col == 'Nombre Campana':
            column_mapping[col] = 'nombre_campana'
    
    if column_mapping:
        df = df.rename(column_mapping)
        print(f"  - Columnas de asignación normalizadas: {df.columns[:10]}...")
    
    return df

def process_blaster_data(df_blaster, df_assignment, output_path, timestamp):
    print("\n=== INICIANDO PROCESAMIENTO BLASTER ===")
    print(f"Registros Blaster: {df_blaster.height}")
    print(f"Registros Asignación: {df_assignment.height}")
    
    df_blaster = normalize_column_names(df_blaster)
    df_blaster = df_blaster.rename({col: col.strip() for col in df_blaster.columns})
    
    print(f"Columnas Blaster después normalización: {df_blaster.columns}")
    
    if 'IDENTIFICACION' in df_blaster.columns:
        df_blaster = df_blaster.rename({'IDENTIFICACION': 'identificacion'})
        df_blaster = df_blaster.with_columns(pl.col('identificacion').cast(pl.Utf8))
        print("  - Renombrado IDENTIFICACION a identificacion")
    elif 'identificacion' in df_blaster.columns:
        df_blaster = df_blaster.with_columns(pl.col('identificacion').cast(pl.Utf8))
        print("  - Columna identificacion ya existe, casteada a Utf8")
    elif 'IDENTI' in df_blaster.columns:
        df_blaster = df_blaster.rename({'IDENTI': 'identificacion'})
        df_blaster = df_blaster.with_columns(pl.col('identificacion').cast(pl.Utf8))
        print("  - Renombrado IDENTI a identificacion")
    else:
        print(f"  - ERROR: No se encontró columna de identificación en Blaster")
        return
    
    if 'ESTADO' in df_blaster.columns:
        print(f"  - Traduciendo estados Blaster. Estados únicos antes: {df_blaster['ESTADO'].unique().to_list()}")
        df_blaster = df_blaster.with_columns(
            pl.col('ESTADO').map_elements(translate_estado_blaster, return_dtype=pl.Utf8).alias('ESTADO')
        )
        print(f"  - Estados únicos después traducción: {df_blaster['ESTADO'].unique().to_list()}")
    
    df_blaster = adjust_estado_for_duration(df_blaster)
    
    if 'cuenta_next' in df_assignment.columns:
        df_assignment = df_assignment.with_columns(
            pl.col('cuenta_next')
            .cast(pl.Utf8)
            .str.replace_all("-", "")
            .str.replace_all(" ", "")
            .alias('cuenta_next')
        )
        print(f"  - cuenta_next normalizada: {df_assignment['cuenta_next'].head(3).to_list()}")
    
    print(f"  - Verificando match de identificaciones...")
    blaster_ids = df_blaster['identificacion'].unique().to_list()
    assignment_ids = df_assignment['cuenta_next'].unique().to_list()
    print(f"    IDs únicos Blaster: {len(blaster_ids)}")
    print(f"    IDs únicos Asignación: {len(assignment_ids)}")
    
    common_ids = set(blaster_ids).intersection(set(assignment_ids))
    print(f"    IDs comunes: {len(common_ids)}")
    if len(common_ids) > 0:
        print(f"    Muestra de IDs comunes: {list(common_ids)[:5]}")
    
    print(f"  - Realizando join...")
    cruce_completo = df_blaster.join(
        df_assignment,
        left_on='identificacion',
        right_on='cuenta_next',
        how='inner'
    )
    print(f"  - Registros después del join: {cruce_completo.height}")
    
    if cruce_completo.height > 0:
        print(f"  - Creando archivo de cruce completo...")
        columnas_a_seleccionar = []
        
        columnas_a_seleccionar.append(pl.lit('BLASTER INTERCOM').alias('CANAL'))
        
        if 'ESTADO' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('ESTADO').alias('ESTADO'))
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('ESTADO'))
        
        if 'FECHA DE MARCACION' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('FECHA DE MARCACION').alias('FECHA_INICIO_ULTIMA_LLAMADA'))
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('FECHA_INICIO_ULTIMA_LLAMADA'))
        
        if 'DURACION' in cruce_completo.columns:
            columnas_a_seleccionar.append(
                pl.col('DURACION')
                .map_elements(format_duration_to_seconds, return_dtype=pl.Int64)
                .alias('DURACION_SEGUNDOS')
            )
        else:
            columnas_a_seleccionar.append(pl.lit(0).alias('DURACION_SEGUNDOS'))
        
        if 'NUMERO MARCADO' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('NUMERO MARCADO').alias('CELULAR'))
        elif 'TELEFONO 1' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('TELEFONO 1').alias('CELULAR'))
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('CELULAR'))
        
        if 'cuenta_next' in cruce_completo.columns:
            columnas_a_seleccionar.append(
                (pl.col('cuenta_next').cast(pl.Utf8).str.replace_all('"', '') + "-").alias('IDENTIFICACION')
            )
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('IDENTIFICACION'))
        
        if 'MARCA' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('MARCA').alias('MARCA'))
        elif 'marca' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('marca').alias('MARCA'))
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('MARCA'))
        
        if 'crm_origen' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('crm_origen').alias('crm_origen'))
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('crm_origen'))
        
        if 'valor_scoring_duplicado_0' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('valor_scoring_duplicado_0').alias('VALOR_SCORING_DUPLICATED_0'))
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('VALOR_SCORING_DUPLICATED_0'))
        
        if 'monitor' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('monitor').alias('MONITOR'))
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('MONITOR'))
        
        if 'rango_saldo' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('rango_saldo').alias('RANGO_SALDO'))
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('RANGO_SALDO'))
        
        if 'nombre_campana' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('nombre_campana').alias('NOMBRE_CAMPANA'))
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('NOMBRE_CAMPANA'))
        
        if 'mejorperfil_mes' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('mejorperfil_mes').alias('MEJORPERFIL_MES'))
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('MEJORPERFIL_MES'))
        
        if 'NOMBRE DE LA CAMPAÑA' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('NOMBRE DE LA CAMPAÑA').alias('NOMBRE DE LA CAMPAÑA'))
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('NOMBRE DE LA CAMPAÑA'))
        
        cruce_seleccionado = cruce_completo.select(columnas_a_seleccionar)
        print(f"  - Columnas seleccionadas: {cruce_seleccionado.columns}")
        
        cruce_seleccionado = cruce_seleccionado.select(COLUMNAS_EFECTIVAS_BLASTER.keys())
        print(f"  - Columnas finales: {cruce_seleccionado.columns}")
        
        cruce_completo_path = output_path / f'blaster_cruce_completo_{timestamp}.csv'
        cruce_seleccionado.write_csv(cruce_completo_path, separator=';', quote_style='never')
        print(f"  - ✅ Archivo guardado: {cruce_completo_path}")
    else:
        print(f"  - ❌ No se encontraron registros en el cruce completo")
    
    print(f"  - Calculando no cruzados...")
    no_cruzado = df_blaster.join(
        df_assignment,
        left_on='identificacion',
        right_on='cuenta_next',
        how='anti'
    )
    print(f"  - Registros no cruzados: {no_cruzado.height}")
    
    estados_efectivos = get_estados_efectivos_blaster()
    print(f"  - Estados efectivos Blaster: {estados_efectivos}")
    
    if cruce_completo.height > 0 and 'ESTADO' in cruce_completo.columns:
        efectivo_vigente = cruce_completo.filter(pl.col('ESTADO').is_in(estados_efectivos))
        cruzado = cruce_completo.filter(~pl.col('ESTADO').is_in(estados_efectivos))
        print(f"  - Registros efectivo vigente: {efectivo_vigente.height}")
        print(f"  - Registros cruzado: {cruzado.height}")
    else:
        efectivo_vigente = cruce_completo
        cruzado = cruce_completo
        print(f"  - No se encontró columna ESTADO o cruce vacío")
    
    if no_cruzado.height > 0:
        print(f"  - Generando archivo de retiradas...")
        retiradas_columns = {}
        for old_name, new_name in COLUMNS_RETIRADAS.items():
            if old_name == 'FECHA DE MARCACION':
                if 'FECHA DE MARCACION' in no_cruzado.columns:
                    retiradas_columns['FECHA DE MARCACION'] = new_name
            elif old_name == 'DURACION':
                if 'DURACION' in no_cruzado.columns:
                    retiradas_columns['DURACION'] = new_name
            elif old_name in no_cruzado.columns:
                retiradas_columns[old_name] = new_name
        
        if retiradas_columns:
            retiradas = no_cruzado.select([
                pl.col(old_name).alias(new_name) for old_name, new_name in retiradas_columns.items()
            ])
            if 'cuenta_next' in retiradas.columns:
                retiradas = retiradas.with_columns(
                    (pl.col('cuenta_next').cast(pl.Utf8).str.replace_all('"', '') + "-").fill_null("")
                )
            retiradas_path = output_path / f'blaster_retiradas_{timestamp}.csv'
            retiradas.write_csv(retiradas_path, separator=';', quote_style='never')
            print(f"  - ✅ Archivo guardado: {retiradas_path}")
    
    if efectivo_vigente.height > 0:
        print(f"  - Generando archivo de efectivo vigente...")
        efectiva_path = output_path / f'blaster_efectivo_vigente_{timestamp}.csv'
        efectivo_vigente.write_csv(efectiva_path, separator=';', quote_style='never')
        print(f"  - ✅ Archivo guardado: {efectiva_path}")
    
    if cruzado.height > 0:
        print(f"  - Generando archivo depurado para cargue...")
        selected_columns = []
        if 'identificacion' in cruzado.columns:
            selected_columns.append(
                (pl.col('identificacion').cast(pl.Utf8).str.replace_all('"', '') + "-").alias('IDENTI')
            )
        if 'TELEFONO 1' in cruzado.columns:
            selected_columns.append(
                pl.col('TELEFONO 1').alias('NUMERO MARCADO')
            )
        if 'MARCA' in cruzado.columns:
            selected_columns.append(
                pl.col('MARCA').alias('MARCA')
            )
        if 'CRM' in cruzado.columns:
            selected_columns.append(
                pl.col('CRM').alias('CRM')
            )
        if selected_columns:
            blaster_depurado = cruzado.select(selected_columns)
            blaster_path = output_path / f'blaster_depurado_cargue_{timestamp}.csv'
            blaster_depurado.write_csv(blaster_path, separator=';', quote_style='never')
            print(f"  - ✅ Archivo guardado: {blaster_path}")
        else:
            print(f"  - ❌ No se seleccionaron columnas para depurado")
    print("=== FIN PROCESAMIENTO BLASTER ===\n")

def process_ivr_saem_data(df_ivr, df_assignment, output_path, timestamp):
    print("\n=== INICIANDO PROCESAMIENTO IVR SAEM ===")
    print(f"Registros IVR: {df_ivr.height}")
    print(f"Registros Asignación: {df_assignment.height}")
    print(f"Columnas IVR: {df_ivr.columns}")
    
    df_ivr = normalize_ivr_saem_columns(df_ivr)
    print(f"Columnas IVR después normalización: {df_ivr.columns}")
    
    if 'identificacion' not in df_ivr.columns:
        print(f"  - ❌ ERROR: No se encontró columna 'identificacion' en IVR SAEM")
        return
    
    if 'cuenta_next' in df_assignment.columns:
        df_assignment = df_assignment.with_columns(
            pl.col('cuenta_next')
            .cast(pl.Utf8)
            .str.replace_all("-", "")
            .str.replace_all(" ", "")
            .alias('cuenta_next')
        )
        print(f"  - cuenta_next normalizada: {df_assignment['cuenta_next'].head(3).to_list()}")
    else:
        print(f"  - ❌ ERROR: No se encontró columna 'cuenta_next' en asignación")
        print(f"    Columnas disponibles: {df_assignment.columns}")
        return
    
    print(f"  - Verificando match de identificaciones...")
    ivr_ids = df_ivr['identificacion'].unique().to_list()
    assignment_ids = df_assignment['cuenta_next'].unique().to_list()
    print(f"    IDs únicos IVR: {len(ivr_ids)}")
    print(f"    IDs únicos Asignación: {len(assignment_ids)}")
    
    common_ids = set(ivr_ids).intersection(set(assignment_ids))
    print(f"    IDs comunes: {len(common_ids)}")
    if len(common_ids) > 0:
        print(f"    Muestra de IDs comunes: {list(common_ids)[:5]}")
    
    print(f"  - Realizando join...")
    cruce_completo = df_ivr.join(
        df_assignment,
        left_on='identificacion',
        right_on='cuenta_next',
        how='inner'
    )
    print(f"  - Registros después del join: {cruce_completo.height}")
    
    if cruce_completo.height > 0:
        print(f"  - Creando archivo de cruce completo...")
        columnas_a_seleccionar = []
        
        columnas_a_seleccionar.append(pl.lit('IVR SAEM').alias('CANAL'))
        
        if 'Mejor_Marcacion' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('Mejor_Marcacion').alias('ESTADO'))
        elif 'MEJOR_MARCACION_NORMALIZADA' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('MEJOR_MARCACION_NORMALIZADA').alias('ESTADO'))
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('ESTADO'))
        
        if 'Fecha_Inicio_Ultima_Llamada' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('Fecha_Inicio_Ultima_Llamada').alias('FECHA_INICIO_ULTIMA_LLAMADA'))
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('FECHA_INICIO_ULTIMA_LLAMADA'))
        
        if 'secounds' in cruce_completo.columns:
            columnas_a_seleccionar.append(
                pl.col('secounds')
                .cast(pl.Int64)
                .fill_null(0)
                .alias('DURACION_SEGUNDOS')
            )
        else:
            columnas_a_seleccionar.append(pl.lit(0).alias('DURACION_SEGUNDOS'))
        
        if 'Celular' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('Celular').alias('CELULAR'))
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('CELULAR'))
        
        if 'identificacion' in cruce_completo.columns:
            columnas_a_seleccionar.append(
                (pl.col('identificacion').cast(pl.Utf8).str.replace_all('"', '') + "-").alias('IDENTIFICACION')
            )
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('IDENTIFICACION'))
        
        if 'MARCA' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('MARCA').alias('MARCA'))
        elif 'marca' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('marca').alias('MARCA'))
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('MARCA'))
        
        if 'crm_origen' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('crm_origen').alias('crm_origen'))
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('crm_origen'))
        
        if 'valor_scoring_duplicated_0' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('valor_scoring_duplicated_0').alias('VALOR_SCORING_DUPLICATED_0'))
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('VALOR_SCORING_DUPLICATED_0'))
        
        if 'monitor' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('monitor').alias('MONITOR'))
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('MONITOR'))
        
        if 'rango_saldo' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('rango_saldo').alias('RANGO_SALDO'))
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('RANGO_SALDO'))
        
        if 'nombre_campana' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('nombre_campana').alias('NOMBRE_CAMPANA'))
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('NOMBRE_CAMPANA'))
        
        if 'mejorperfil_mes' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('mejorperfil_mes').alias('MEJORPERFIL_MES'))
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('MEJORPERFIL_MES'))
        
        columnas_a_seleccionar.append(pl.lit('ivr masivo').alias('NOMBRE DE LA CAMPAÑA'))
        
        cruce_seleccionado = cruce_completo.select(columnas_a_seleccionar)
        print(f"  - Columnas seleccionadas: {cruce_seleccionado.columns}")
        
        cruce_seleccionado = cruce_seleccionado.select(COLUMNAS_EFECTIVAS_SAEM.keys())
        print(f"  - Columnas finales: {cruce_seleccionado.columns}")
        
        cruce_completo_path = output_path / f'ivr_saem_cruce_completo_{timestamp}.csv'
        cruce_seleccionado.write_csv(cruce_completo_path, separator=';', quote_style='never')
        print(f"  - ✅ Archivo guardado: {cruce_completo_path}")
    else:
        print(f"  - ❌ No se encontraron registros en el cruce completo")
    
    no_cruzado = df_ivr.join(
        df_assignment,
        left_on='identificacion',
        right_on='cuenta_next',
        how='anti'
    )
    print(f"  - Registros no cruzados: {no_cruzado.height}")
    
    estados_efectivos = get_estados_efectivos_saem()
    print(f"  - Estados efectivos SAEM: {estados_efectivos}")
    
    if cruce_completo.height > 0:
        if 'Mejor_Marcacion' in cruce_completo.columns and 'secounds' in cruce_completo.columns:
            efectivo_vigente = cruce_completo.filter(
                (pl.col('Mejor_Marcacion').is_in(estados_efectivos)) &
                (pl.col('secounds') > 4)
            )
            cruzado = cruce_completo.filter(
                ~((pl.col('Mejor_Marcacion').is_in(estados_efectivos)) &
                  (pl.col('secounds') > 4))
            )
            print(f"  - Registros efectivo vigente (con duración > 4s): {efectivo_vigente.height}")
            print(f"  - Registros cruzado: {cruzado.height}")
        elif 'Mejor_Marcacion' in cruce_completo.columns:
            efectivo_vigente = cruce_completo.filter(
                pl.col('Mejor_Marcacion').is_in(estados_efectivos)
            )
            cruzado = cruce_completo.filter(
                ~pl.col('Mejor_Marcacion').is_in(estados_efectivos)
            )
            print(f"  - Registros efectivo vigente (sin filtro duración): {efectivo_vigente.height}")
            print(f"  - Registros cruzado: {cruzado.height}")
        else:
            efectivo_vigente = cruce_completo
            cruzado = cruce_completo.filter(pl.col('identificacion').is_not_null())
            print(f"  - No se encontró columna Mejor_Marcacion")
    else:
        efectivo_vigente = cruce_completo
        cruzado = cruce_completo
    
    if no_cruzado.height > 0:
        print(f"  - Generando archivo de retiradas...")
        if 'Identificacion' not in no_cruzado.columns and 'identificacion' in no_cruzado.columns:
            no_cruzado = no_cruzado.with_columns(
                pl.col('identificacion').alias('Identificacion')
            )
        retiradas_path = output_path / f'ivr_saem_retiradas_{timestamp}.csv'
        no_cruzado.write_csv(retiradas_path, separator=';', quote_style='never')
        print(f"  - ✅ Archivo guardado: {retiradas_path}")
    
    if efectivo_vigente.height > 0:
        print(f"  - Generando archivo de efectivo vigente...")
        if 'Identificacion' not in efectivo_vigente.columns and 'identificacion' in efectivo_vigente.columns:
            efectivo_vigente = efectivo_vigente.with_columns(
                pl.col('identificacion').alias('Identificacion')
            )
        efectiva_path = output_path / f'ivr_saem_efectivo_vigente_{timestamp}.csv'
        efectivo_vigente.write_csv(efectiva_path, separator=';', quote_style='never')
        print(f"  - ✅ Archivo guardado: {efectiva_path}")
    
    if cruzado.height > 0:
        print(f"  - Generando archivo depurado para cargue...")
        selected_columns = []
        if 'Celular' in cruzado.columns:
            selected_columns.append(
                pl.col('Celular').alias('NUMERO MARCADO')
            )
        elif 'NUMERO MARCADO' in cruzado.columns:
            selected_columns.append(
                pl.col('NUMERO MARCADO').alias('NUMERO MARCADO')
            )
        if 'identificacion' in cruzado.columns:
            selected_columns.append(
                (pl.col('identificacion').cast(pl.Utf8).str.replace_all('"', '') + "-").alias('IDENTI')
            )
        elif 'Identificacion' in cruzado.columns:
            selected_columns.append(
                (pl.col('Identificacion').cast(pl.Utf8).str.replace_all('"', '') + "-").alias('IDENTI')
            )
        elif 'IDENTIFICACION' in cruzado.columns:
            selected_columns.append(
                (pl.col('IDENTIFICACION').cast(pl.Utf8).str.replace_all('"', '') + "-").alias('IDENTI')
            )
        if selected_columns:
            depurado = cruzado.select(selected_columns)
            depurado_path = output_path / f'ivr_saem_depurado_cargue_{timestamp}.csv'
            depurado.write_csv(depurado_path, separator=';', quote_style='never')
            print(f"  - ✅ Archivo guardado: {depurado_path}")
        else:
            print(f"  - ❌ No se seleccionaron columnas para depurado")
    print("=== FIN PROCESAMIENTO IVR SAEM ===\n")
            
def process_ivr_data(input_folder: str, output_folder: str):
    print(f"\n{'='*60}")
    print(f"INICIANDO PROCESO DE DATOS IVR")
    print(f"{'='*60}")
    print(f"Carpeta de entrada: {input_folder}")
    print(f"Carpeta de salida: {output_folder}")
    
    input_path = Path(input_folder)
    output_path = Path(output_folder)
    
    if not input_path.exists():
        print(f"❌ ERROR: La carpeta de entrada no existe: {input_path}")
        return
    
    print(f"✅ Carpeta de entrada existe: {input_path}")
    
    output_path.mkdir(parents=True, exist_ok=True)
    print(f"✅ Carpeta de salida creada/verificada: {output_path}")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    print(f"Timestamp: {timestamp}")
    
    csv_files = list(input_path.glob("*.csv"))
    print(f"Archivos CSV encontrados: {len(csv_files)}")
    
    if not csv_files:
        print(f"❌ ERROR: No se encontraron archivos CSV en {input_path}")
        return
    
    for file in csv_files:
        print(f"  - {file.name}")
    
    df_blaster = None
    df_ivr_saem = None
    df_assignment = None
    
    print("\n--- Clasificando archivos ---")
    for csv_file in csv_files:
        try:
            print(f"\nProcesando: {csv_file.name}")
            
            if "asignacion_claro" in csv_file.name.lower():
                print(f"  📁 Detectado: Archivo de asignación")
                try:
                    df = pl.read_csv(
                        str(csv_file), 
                        separator='\t',
                        infer_schema_length=0,
                        try_parse_dates=True,
                        ignore_errors=True
                    )
                    print(f"    ✅ Leído con separador tabulación")
                except:
                    try:
                        df = pl.read_csv(
                            str(csv_file), 
                            separator=';',
                            infer_schema_length=0,
                            try_parse_dates=True,
                            ignore_errors=True
                        )
                        print(f"    ✅ Leído con separador punto y coma")
                    except:
                        df = pl.read_csv(
                            str(csv_file), 
                            separator=',',
                            infer_schema_length=0,
                            try_parse_dates=True,
                            ignore_errors=True
                        )
                        print(f"    ✅ Leído con separador coma")
                
                print(f"    Filas: {df.height}, Columnas: {len(df.columns)}")
                print(f"    Primeras columnas: {df.columns[:10]}")
                
                df = normalize_assignment_columns(df)
                
                if df_assignment is None:
                    df_assignment = df
                    print(f"    ✅ Asignación inicial creada con {df.height} registros")
                else:
                    df_assignment = pl.concat([df_assignment, df], how="diagonal")
                    print(f"    ✅ Asignación concatenada. Total filas: {df_assignment.height}")
                    
            else:
                print(f"  📁 Detectado: Archivo de datos")
                try:
                    df = pl.read_csv(str(csv_file), separator=';', ignore_errors=True)
                    print(f"    ✅ Leído con separador punto y coma")
                except Exception as e:
                    try:
                        df = pl.read_csv(str(csv_file), separator=',', ignore_errors=True)
                        print(f"    ✅ Leído con separador coma")
                    except Exception as e2:
                        print(f"    ❌ Error al leer CSV: {e}")
                        continue
                
                print(f"    Filas: {df.height}, Columnas: {df.columns}")
                
                df = normalize_column_names(df)
                
                if "LEAD ID" in df.columns:
                    print(f"    ✅ Identificado como BLASTER (contiene 'LEAD ID')")
                    if df_blaster is None:
                        df_blaster = df
                        print(f"    Blaster inicial creado con {df.height} registros")
                    else:
                        df_blaster = pl.concat([df_blaster, df], how="diagonal")
                        print(f"    Blaster concatenado. Total filas: {df_blaster.height}")
                elif "Identificacion" in df.columns or "IDENTIFICACION" in df.columns or "Celular" in df.columns:
                    print(f"    ✅ Identificado como IVR SAEM")
                    if df_ivr_saem is None:
                        df_ivr_saem = df
                        print(f"    IVR SAEM inicial creado con {df.height} registros")
                    else:
                        df_ivr_saem = pl.concat([df_ivr_saem, df], how="diagonal")
                        print(f"    IVR SAEM concatenado. Total filas: {df_ivr_saem.height}")
                else:
                    print(f"    ❌ No se pudo identificar el tipo de archivo")
                    print(f"    Columnas disponibles: {df.columns}")
        except Exception as e:
            print(f"❌ Error procesando archivo {csv_file.name}: {e}")
            traceback.print_exc()
            continue
    
    print("\n--- Resumen de datos cargados ---")
    if df_assignment is not None:
        print(f"✅ Asignación: {df_assignment.height} registros")
        print(f"   Columnas: {df_assignment.columns[:15]}...")
        print(f"   ¿Tiene columna 'cuenta_next'? {'✅' if 'cuenta_next' in df_assignment.columns else '❌'}")
        print(f"   ¿Tiene columna 'liquidacion'? {'✅' if 'liquidacion' in df_assignment.columns else '❌'}")
    else:
        print(f"❌ ERROR: No se cargó ningún archivo de asignación")
        return
    
    if df_blaster is not None:
        print(f"✅ Blaster: {df_blaster.height} registros")
        print(f"   Columnas: {df_blaster.columns[:10]}...")
    else:
        print(f"⚠️ No se encontraron archivos Blaster")
    
    if df_ivr_saem is not None:
        print(f"✅ IVR SAEM: {df_ivr_saem.height} registros")
        print(f"   Columnas: {df_ivr_saem.columns[:10]}...")
    else:
        print(f"⚠️ No se encontraron archivos IVR SAEM")
    
    if df_blaster is not None:
        process_blaster_data(df_blaster, df_assignment, output_path, timestamp)
    else:
        print(f"\n❌ No se procesa Blaster por falta de datos")
    
    if df_ivr_saem is not None:
        process_ivr_saem_data(df_ivr_saem, df_assignment, output_path, timestamp)
    else:
        print(f"\n❌ No se procesa IVR SAEM por falta de datos")
    
    print(f"\n{'='*60}")
    print(f"PROCESO COMPLETADO")
    print(f"{'='*60}")

if __name__ == "__main__":
    input_folder = r"C:\Users\c.desarrollo\Downloads\blaster"
    output_folder = r"C:\Users\c.desarrollo\Downloads"
    process_ivr_data(input_folder, output_folder)