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

COLUMNS_EFECTIVA = {
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
    'TELEFONO 1': 'TELEFONO 1',
    'numeromarcado_ultimapromesa': 'numeromarcado_ultimapromesa',
    'mejorperfil': 'mejorperfil',
    'ultimoperfil': 'ultimoperfil',
    'mejorperfil_mes': 'mejorperfil_mes',
    'ultimoperfil_mes': 'ultimoperfil_mes',
    'fechaultimoperfil': 'fechaultimoperfil',
    'descuento': 'descuento',
    'fechanogestion': 'fechanogestion',
    'fechapagossinaplicar': 'fechapagossinaplicar',
    'valor_descuento': 'valor_descuento',
    'flppp': 'flppp',
    'tipo_pago': 'tipo_pago',
    'nombre_campana': 'nombre_campana',
    'r_c_end': 'r_c_end',
    'r_cvcob': 'r_cvcob',
    'tipo_cte_tu': 'tipo_cte_tu'
}

COLUMNS_BLASTER_DEPURADO = {
    'identificacion': 'IDENTI',
    'TELEFONO 1': 'NUMERO MARCADO',
    'MARCA': 'MARCA',
    'CRM': 'CRM'
}

COLUMNS_IVR_SAEM_RETIRADAS = {
    'Id': 'ID',
    'Celular': 'CELULAR',
    'Identificacion': 'cuenta_next',
    'Texto': 'TEXTO',
    'Fecha_Inicio_Ultima_Llamada': 'FECHA_INICIO_ULTIMA_LLAMADA',
    'Fecha_Fin_Ultima_Llamada': 'FECHA_FIN_ULTIMA_LLAMADA',
    'Resultado_Ultima_Llamada': 'RESULTADO_ULTIMA_LLAMADA',
    'secounds': 'DURACION_SEGUNDÓS',
    'Marcacion_1_Fecha_Inicio': 'MARCACION_1_FECHA_INICIO',
    'Marcacion_1_Fecha_Fin': 'MARCACION_1_FECHA_FIN',
    'Marcacion_1': 'MARCACION_1',
    'Marcacion_2_Fecha_Inicio': 'MARCACION_2_FECHA_INICIO',
    'Marcacion_2_Fecha_Fin': 'MARCACION_2_FECHA_FIN',
    'Marcacion_2': 'MARCACION_2',
    'Marcacion_3_Fecha_Inicio': 'MARCACION_3_FECHA_INICIO',
    'Marcacion_3_Fecha_Fin': 'MARCACION_3_FECHA_FIN',
    'Marcacion_3': 'MARCACION_3',
    'Marcacion_4_Fecha_Inicio': 'MARCACION_4_FECHA_INICIO',
    'Marcacion_4_Fecha_Fin': 'MARCACION_4_FECHA_FIN',
    'Marcacion_4': 'MARCACION_4',
    'Marcacion_5_Fecha_Inicio': 'MARCACION_5_FECHA_INICIO',
    'Marcacion_5_Fecha_Fin': 'MARCACION_5_FECHA_FIN',
    'Marcacion_5': 'MARCACION_5',
    'Mejor_Marcacion': 'MEJOR_MARCACION'
}

COLUMNS_IVR_SAEM_EFECTIVA = {
    'Id': 'ID',
    'Celular': 'CELULAR',
    'Identificacion': 'cuenta_next',
    'Texto': 'TEXTO',
    'Fecha_Inicio_Ultima_Llamada': 'FECHA_INICIO_ULTIMA_LLAMADA',
    'Fecha_Fin_Ultima_Llamada': 'FECHA_FIN_ULTIMA_LLAMADA',
    'Resultado_Ultima_Llamada': 'RESULTADO_ULTIMA_LLAMADA',
    'secounds': 'DURACION_SEGUNDÓS',
    'Marcacion_1_Fecha_Inicio': 'MARCACION_1_FECHA_INICIO',
    'Marcacion_1_Fecha_Fin': 'MARCACION_1_FECHA_FIN',
    'Marcacion_1': 'MARCACION_1',
    'Marcacion_2_Fecha_Inicio': 'MARCACION_2_FECHA_INICIO',
    'Marcacion_2_Fecha_Fin': 'MARCACION_2_FECHA_FIN',
    'Marcacion_2': 'MARCACION_2',
    'Marcacion_3_Fecha_Inicio': 'MARCACION_3_FECHA_INICIO',
    'Marcacion_3_Fecha_Fin': 'MARCACION_3_FECHA_FIN',
    'Marcacion_3': 'MARCACION_3',
    'Marcacion_4_Fecha_Inicio': 'MARCACION_4_FECHA_INICIO',
    'Marcacion_4_Fecha_Fin': 'MARCACION_4_FECHA_FIN',
    'Marcacion_4': 'MARCACION_4',
    'Marcacion_5_Fecha_Inicio': 'MARCACION_5_FECHA_INICIO',
    'Marcacion_5_Fecha_Fin': 'MARCACION_5_FECHA_FIN',
    'Marcacion_5': 'MARCACION_5',
    'Mejor_Marcacion': 'MEJOR_MARCACION'
}

COLUMNS_IVR_SAEM_DEPURADO = {
    'Celular': 'NUMERO MARCADO',
    'Identificacion': 'IDENTI'
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

def parse_duration_to_seconds(duration_str):
    if duration_str is None:
        return 0
    try:
        duration_str = str(duration_str).strip()
        if duration_str.isdigit():
            return int(duration_str)
        parts = duration_str.split(':')
        if len(parts) == 3:
            hours = int(parts[0]) if parts[0].isdigit() else 0
            minutes = int(parts[1]) if parts[1].isdigit() else 0
            seconds = int(parts[2]) if parts[2].isdigit() else 0
            return hours * 3600 + minutes * 60 + seconds
        elif len(parts) == 2:
            minutes = int(parts[0]) if parts[0].isdigit() else 0
            seconds = int(parts[1]) if parts[1].isdigit() else 0
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
        .map_elements(parse_duration_to_seconds, return_dtype=pl.Int64)
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
    
    cruzado = df_blaster.join(
        df_assignment,
        left_on='identificacion',
        right_on='cuenta',
        how='inner'
    )
    no_cruzado = df_blaster.join(
        df_assignment,
        left_on='identificacion',
        right_on='cuenta',
        how='anti'
    )
    
    estados_efectivos = get_estados_efectivos_blaster()
    if 'ESTADO' in cruzado.columns:
        efectivo_vigente = cruzado.filter(pl.col('ESTADO').is_in(estados_efectivos))
        cruzado = cruzado.filter(~pl.col('ESTADO').is_in(estados_efectivos))
    else:
        efectivo_vigente = cruzado
        cruzado = cruzado.filter(~pl.col('ESTADO').is_in(estados_efectivos))
    
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
                pl.col('cuenta_next').str.replace_all('"', '') + "-"
            )
        retiradas_path = output_path / f'blaster_retiradas_{timestamp}.csv'
        retiradas.write_csv(retiradas_path, separator=';', quote_style='never')
    
    if efectivo_vigente.height > 0:
        efectiva_columns = {}
        for old_name, new_name in COLUMNS_EFECTIVA.items():
            if old_name == 'FECHA DE MARCACION':
                if 'FECHA DE MARCACION' in efectivo_vigente.columns:
                    efectiva_columns['FECHA DE MARCACION'] = new_name
            elif old_name == 'DURACION':
                if 'DURACION' in efectivo_vigente.columns:
                    efectiva_columns['DURACION'] = new_name
            elif old_name in efectivo_vigente.columns:
                efectiva_columns[old_name] = new_name
        
        base_efectiva = efectivo_vigente.select([
            pl.col(old_name).alias(new_name) for old_name, new_name in efectiva_columns.items()
        ])
        if 'cuenta_next' in base_efectiva.columns:
            base_efectiva = base_efectiva.with_columns(
                pl.col('cuenta_next').str.replace_all('"', '') + "-"
            )
        efectiva_path = output_path / f'blaster_efectivo_vigente_{timestamp}.csv'
        base_efectiva.write_csv(efectiva_path, separator=';', quote_style='never')
    
    if cruzado.height > 0:
        selected_columns = []
        if 'identificacion' in cruzado.columns:
            selected_columns.append(
                pl.col('identificacion').str.replace_all('"', '').alias('IDENTI')
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
    
    cruzado = df_ivr.join(
        df_assignment,
        left_on='identificacion',
        right_on='cuenta',
        how='inner'
    )
    no_cruzado = df_ivr.join(
        df_assignment,
        left_on='identificacion',
        right_on='cuenta',
        how='anti'
    )
    
    estados_efectivos = get_estados_efectivos_saem()
    if 'Mejor_Marcacion' in cruzado.columns and 'secounds' in cruzado.columns:
        efectivo_vigente = cruzado.filter(
            (pl.col('Mejor_Marcacion').is_in(estados_efectivos)) &
            (pl.col('secounds') > 4)
        )
        cruzado = cruzado.filter(
            ~((pl.col('Mejor_Marcacion').is_in(estados_efectivos)) &
              (pl.col('secounds') > 4))
        )
    elif 'Mejor_Marcacion' in cruzado.columns:
        efectivo_vigente = cruzado.filter(
            pl.col('Mejor_Marcacion').is_in(estados_efectivos)
        )
        cruzado = cruzado.filter(
            ~pl.col('Mejor_Marcacion').is_in(estados_efectivos)
        )
    else:
        efectivo_vigente = cruzado
        cruzado = cruzado.filter(pl.col('identificacion').is_not_null())
    
    if no_cruzado.height > 0:
        if 'Identificacion' not in no_cruzado.columns and 'identificacion' in no_cruzado.columns:
            no_cruzado = no_cruzado.with_columns(
                pl.col('identificacion').alias('Identificacion')
            )
        
        retiradas_columns = {k: v for k, v in COLUMNS_IVR_SAEM_RETIRADAS.items() if k in no_cruzado.columns}
        retiradas = no_cruzado.select([
            pl.col(old_name).alias(new_name) for old_name, new_name in retiradas_columns.items()
        ])
        if 'cuenta_next' in retiradas.columns:
            retiradas = retiradas.with_columns(
                pl.col('cuenta_next').str.replace_all('"', '') + "-"
            )
        retiradas_path = output_path / f'ivr_saem_retiradas_{timestamp}.csv'
        retiradas.write_csv(retiradas_path, separator=';', quote_style='never')
    
    if efectivo_vigente.height > 0:
        if 'Identificacion' not in efectivo_vigente.columns and 'identificacion' in efectivo_vigente.columns:
            efectivo_vigente = efectivo_vigente.with_columns(
                pl.col('identificacion').alias('Identificacion')
            )
        
        efectiva_columns = {k: v for k, v in COLUMNS_IVR_SAEM_EFECTIVA.items() if k in efectivo_vigente.columns}
        base_efectiva = efectivo_vigente.select([
            pl.col(old_name).alias(new_name) for old_name, new_name in efectiva_columns.items()
        ])
        if 'cuenta_next' in base_efectiva.columns:
            base_efectiva = base_efectiva.with_columns(
                pl.col('cuenta_next').str.replace_all('"', '') + "-"
            )
        efectiva_path = output_path / f'ivr_saem_efectivo_vigente_{timestamp}.csv'
        base_efectiva.write_csv(efectiva_path, separator=';', quote_style='never')
    
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
                pl.col('identificacion')
                .str.replace_all('"', '')
                .alias('IDENTI')
            )
        elif 'Identificacion' in cruzado.columns:
            selected_columns.append(
                pl.col('Identificacion')
                .str.replace_all('"', '')
                .alias('IDENTI')
            )
        elif 'IDENTIFICACION' in cruzado.columns:
            selected_columns.append(
                pl.col('IDENTIFICACION')
                .str.replace_all('"', '')
                .alias('IDENTI')
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
                except Exception as e:
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
    if 'cuenta' in df_assignment.columns:
        df_assignment = df_assignment.with_columns(
            pl.col('cuenta')
            .cast(pl.Utf8)
            .str.replace_all("-", "")
            .alias('cuenta')
        )
    
    if df_blaster is not None:
        process_blaster_data(df_blaster, df_assignment, output_path, timestamp)
    
    if df_ivr_saem is not None:
        process_ivr_saem_data(df_ivr_saem, df_assignment, output_path, timestamp)