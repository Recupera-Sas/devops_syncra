import polars as pl
from pathlib import Path
from datetime import datetime
import sys

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
    'cuenta2': 'cuenta2',
    'MARCA': 'MARCA',
    'ORIGEN': 'ORIGEN',
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
    'cuenta2': 'cuenta2',
    'MARCA': 'MARCA',
    'ORIGEN': 'ORIGEN',
    'VALOR_SCORING_DUPLICATED_0': 'VALOR_SCORING_DUPLICATED_0',
    'MONITOR': 'MONITOR',
    'RANGO_SALDO': 'RANGO_SALDO',
    'NOMBRE_CAMPANA': 'NOMBRE_CAMPANA',
    'MEJORPERFIL_MES': 'MEJORPERFIL_MES',
    'NOMBRE DE LA CAMPAÑA': 'NOMBRE DE LA CAMPAÑA'
}

def normalize_column_names(df):
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
    except Exception:
        return 0

def adjust_estado_for_duration(df):
    if 'ESTADO' not in df.columns or 'DURACION' not in df.columns:
        return df

    df = df.with_columns(
        pl.col('DURACION')
        .map_elements(format_duration_to_seconds, return_dtype=pl.Int64)
        .alias('_duration_seconds')
    )
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
        df = df.with_columns(
            pl.col('Celular')
            .cast(pl.Utf8)
            .str.replace_all('"', '')
            .str.slice(2)
            .alias('Celular')
        )
    return df

def normalize_ivr_saem_columns(df):
    if 'Identificacion' in df.columns:
        df = df.rename({'Identificacion': 'identificacion'})
        df = df.with_columns(pl.col('identificacion').cast(pl.Utf8))
    elif 'IDENTIFICACION' in df.columns:
        df = df.rename({'IDENTIFICACION': 'identificacion'})
        df = df.with_columns(pl.col('identificacion').cast(pl.Utf8))
    df = process_celular_ivr_saem(df)
    if 'Mejor_Marcacion' in df.columns:
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

def process_blaster_data(df_blaster, df_assignment, output_path, timestamp):
    df_blaster = normalize_column_names(df_blaster)
    df_blaster = df_blaster.rename({col: col.strip() for col in df_blaster.columns})
    
    if 'IDENTIFICACION' in df_blaster.columns:
        df_blaster = df_blaster.rename({'IDENTIFICACION': 'identificacion'})
        df_blaster = df_blaster.with_columns(pl.col('identificacion').cast(pl.Utf8))
    elif 'identificacion' in df_blaster.columns:
        df_blaster = df_blaster.with_columns(pl.col('identificacion').cast(pl.Utf8))
    elif 'IDENTI' in df_blaster.columns:
        df_blaster = df_blaster.rename({'IDENTI': 'identificacion'})
        df_blaster = df_blaster.with_columns(pl.col('identificacion').cast(pl.Utf8))
    
    if 'ESTADO' in df_blaster.columns:
        df_blaster = df_blaster.with_columns(
            pl.col('ESTADO').map_elements(translate_estado_blaster, return_dtype=pl.Utf8).alias('ESTADO')
        )
    
    df_blaster = adjust_estado_for_duration(df_blaster)
    
    if 'cuenta' not in df_assignment.columns and 'cuenta2' in df_assignment.columns:
        df_assignment = df_assignment.with_columns(
            pl.col('cuenta2').alias('cuenta')
        )
    
    if 'cuenta' in df_assignment.columns:
        df_assignment = df_assignment.with_columns(
            pl.col('cuenta')
            .cast(pl.Utf8)
            .str.replace_all("-", "")
            .alias('cuenta')
        )
    
    cruce_completo = df_blaster.join(
        df_assignment,
        left_on='identificacion',
        right_on='cuenta',
        how='inner'
    )
    
    if cruce_completo.height > 0:
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
        
        if 'cuenta' in cruce_completo.columns:
            columnas_a_seleccionar.append(
                (pl.col('cuenta').str.replace_all('"', '') + "-").alias('IDENTIFICACION')
            )
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('IDENTIFICACION'))
        
        if 'cuenta2' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('cuenta2').alias('cuenta2'))
        elif 'cuenta' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('cuenta').alias('cuenta2'))
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('cuenta2'))
        
        if 'MARCA' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('MARCA').alias('MARCA'))
        elif 'marca' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('marca').alias('MARCA'))
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('MARCA'))
        
        if 'origen' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('origen').alias('ORIGEN'))
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('ORIGEN'))
        
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
        
        cruce_seleccionado = cruce_seleccionado.select(COLUMNAS_EFECTIVAS_BLASTER.keys())
        
        cruce_completo_path = output_path / f'blaster_cruce_completo_{timestamp}.csv'
        cruce_seleccionado.write_csv(cruce_completo_path, separator=';', quote_style='never')
    
    no_cruzado = df_blaster.join(
        df_assignment,
        left_on='identificacion',
        right_on='cuenta',
        how='anti'
    )
    
    estados_efectivos = get_estados_efectivos_blaster()
    if 'ESTADO' in cruce_completo.columns:
        efectivo_vigente = cruce_completo.filter(pl.col('ESTADO').is_in(estados_efectivos))
        cruzado = cruce_completo.filter(~pl.col('ESTADO').is_in(estados_efectivos))
    else:
        efectivo_vigente = cruce_completo
        cruzado = cruce_completo.filter(~pl.col('ESTADO').is_in(estados_efectivos))
    
    if no_cruzado.height > 0:
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
        
        retiradas = no_cruzado.select([
            pl.col(old_name).alias(new_name) for old_name, new_name in retiradas_columns.items()
        ])
        if 'cuenta_next' in retiradas.columns:
            retiradas = retiradas.with_columns(
                (pl.col('cuenta_next').str.replace_all('"', '') + "-").fill_null("")
            )
        retiradas_path = output_path / f'blaster_retiradas_{timestamp}.csv'
        retiradas.write_csv(retiradas_path, separator=';', quote_style='never')
    
    if efectivo_vigente.height > 0:
        efectiva_path = output_path / f'blaster_efectivo_vigente_{timestamp}.csv'
        efectivo_vigente.write_csv(efectiva_path, separator=';', quote_style='never')
    
    if cruzado.height > 0:
        selected_columns = []
        if 'identificacion' in cruzado.columns:
            selected_columns.append(
                (pl.col('identificacion').str.replace_all('"', '') + "-").alias('IDENTI')
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

def process_ivr_saem_data(df_ivr, df_assignment, output_path, timestamp):
    df_ivr = normalize_ivr_saem_columns(df_ivr)
    
    if 'cuenta' not in df_assignment.columns and 'cuenta2' in df_assignment.columns:
        df_assignment = df_assignment.with_columns(
            pl.col('cuenta2').alias('cuenta')
        )
    
    if 'cuenta' in df_assignment.columns:
        df_assignment = df_assignment.with_columns(
            pl.col('cuenta')
            .cast(pl.Utf8)
            .str.replace_all("-", "")
            .alias('cuenta')
        )
    
    cruce_completo = df_ivr.join(
        df_assignment,
        left_on='identificacion',
        right_on='cuenta',
        how='inner'
    )
    
    if cruce_completo.height > 0:
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
                (pl.col('identificacion').str.replace_all('"', '') + "-").alias('IDENTIFICACION')
            )
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('IDENTIFICACION'))
        
        if 'cuenta2' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('cuenta2').alias('cuenta2'))
        elif 'cuenta' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('cuenta').alias('cuenta2'))
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('cuenta2'))
        
        if 'MARCA' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('MARCA').alias('MARCA'))
        elif 'marca' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('marca').alias('MARCA'))
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('MARCA'))
        
        if 'origen' in cruce_completo.columns:
            columnas_a_seleccionar.append(pl.col('origen').alias('ORIGEN'))
        else:
            columnas_a_seleccionar.append(pl.lit('').alias('ORIGEN'))
        
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
        
        cruce_seleccionado = cruce_seleccionado.select(COLUMNAS_EFECTIVAS_SAEM.keys())
        
        cruce_completo_path = output_path / f'ivr_saem_cruce_completo_{timestamp}.csv'
        cruce_seleccionado.write_csv(cruce_completo_path, separator=';', quote_style='never')
    
    no_cruzado = df_ivr.join(
        df_assignment,
        left_on='identificacion',
        right_on='cuenta',
        how='anti'
    )
    
    estados_efectivos = get_estados_efectivos_saem()
    if 'Mejor_Marcacion' in cruce_completo.columns and 'secounds' in cruce_completo.columns:
        efectivo_vigente = cruce_completo.filter(
            (pl.col('Mejor_Marcacion').is_in(estados_efectivos)) &
            (pl.col('secounds') > 4)
        )
        cruzado = cruce_completo.filter(
            ~((pl.col('Mejor_Marcacion').is_in(estados_efectivos)) &
              (pl.col('secounds') > 4))
        )
    elif 'Mejor_Marcacion' in cruce_completo.columns:
        efectivo_vigente = cruce_completo.filter(
            pl.col('Mejor_Marcacion').is_in(estados_efectivos)
        )
        cruzado = cruce_completo.filter(
            ~pl.col('Mejor_Marcacion').is_in(estados_efectivos)
        )
    else:
        efectivo_vigente = cruce_completo
        cruzado = cruce_completo.filter(pl.col('identificacion').is_not_null())
    
    if no_cruzado.height > 0:
        if 'Identificacion' not in no_cruzado.columns and 'identificacion' in no_cruzado.columns:
            no_cruzado = no_cruzado.with_columns(
                pl.col('identificacion').alias('Identificacion')
            )
        retiradas_path = output_path / f'ivr_saem_retiradas_{timestamp}.csv'
        no_cruzado.write_csv(retiradas_path, separator=';', quote_style='never')
    
    if efectivo_vigente.height > 0:
        if 'Identificacion' not in efectivo_vigente.columns and 'identificacion' in efectivo_vigente.columns:
            efectivo_vigente = efectivo_vigente.with_columns(
                pl.col('identificacion').alias('Identificacion')
            )
        efectiva_path = output_path / f'ivr_saem_efectivo_vigente_{timestamp}.csv'
        efectivo_vigente.write_csv(efectiva_path, separator=';', quote_style='never')
    
    if cruzado.height > 0:
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
                (pl.col('identificacion').str.replace_all('"', '') + "-").alias('IDENTI')
            )
        elif 'Identificacion' in cruzado.columns:
            selected_columns.append(
                (pl.col('Identificacion').str.replace_all('"', '') + "-").alias('IDENTI')
            )
        elif 'IDENTIFICACION' in cruzado.columns:
            selected_columns.append(
                (pl.col('IDENTIFICACION').str.replace_all('"', '') + "-").alias('IDENTI')
            )
        if selected_columns:
            depurado = cruzado.select(selected_columns)
            depurado_path = output_path / f'ivr_saem_depurado_cargue_{timestamp}.csv'
            depurado.write_csv(depurado_path, separator=';', quote_style='never')
            
def process_ivr_data(input_folder: str, output_folder: str):
    input_path = Path(input_folder)
    output_path = Path(output_folder)
    if not input_path.exists():
        return
    
    output_path.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    
    csv_files = list(input_path.glob("*.csv"))
    if not csv_files:
        return
    
    df_blaster = None
    df_ivr_saem = None
    df_assignment = None
    
    for csv_file in csv_files:
        try:
            if "reporte_clientes" in csv_file.name.lower():
                df = pl.read_csv(
                    str(csv_file), 
                    separator=';',
                    infer_schema_length=0,
                    try_parse_dates=True,
                    ignore_errors=True
                )
                if "cant_servicios" in df.columns:
                    if df_assignment is None:
                        df_assignment = df
                    else:
                        df_assignment = pl.concat([df_assignment, df], how="diagonal")
            else:
                try:
                    df = pl.read_csv(str(csv_file), separator=';', ignore_errors=True)
                except Exception:
                    continue
                
                df = normalize_column_names(df)
                
                if "LEAD ID" in df.columns:
                    if df_blaster is None:
                        df_blaster = df
                    else:
                        df_blaster = pl.concat([df_blaster, df], how="diagonal")
                elif "Identificacion" in df.columns or "IDENTIFICACION" in df.columns or "Celular" in df.columns:
                    if df_ivr_saem is None:
                        df_ivr_saem = df
                    else:
                        df_ivr_saem = pl.concat([df_ivr_saem, df], how="diagonal")
        except Exception:
            continue
    
    if df_assignment is None:
        return
    
    df_assignment = df_assignment.rename({col: col.strip() for col in df_assignment.columns})
    
    if df_blaster is not None:
        process_blaster_data(df_blaster, df_assignment, output_path, timestamp)
    
    if df_ivr_saem is not None:
        process_ivr_saem_data(df_ivr_saem, df_assignment, output_path, timestamp)