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

def translate_estado(value):
    translations = {
        'ANSWERED': 'CONTESTADA',
        'NO ANSWER': 'NO CONTESTA',
        'BUZON': 'BUZON DE VOZ',
        'BUSY': 'OCUPADO',
        'FAILED': 'FALLIDA'
    }
    return translations.get(value, value)

def process_ivr_data(input_folder: str, output_folder: str):
    input_path = Path(input_folder)
    output_path = Path(output_folder)
    
    if not input_path.exists():
        print(f"❌ Error: La carpeta '{input_folder}' no existe.")
        return
    
    output_path.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    csv_files = list(input_path.glob("*.csv"))
    
    if not csv_files:
        print(f"⚠️ No hay archivos CSV en '{input_folder}'")
        return
    
    df_blaster = None
    df_assignment = None
    
    for csv_file in csv_files:
        try:
            if "reporte_clientes" in csv_file.name.lower():
                df = pl.read_csv(
                    str(csv_file), 
                    separator=';',
                    infer_schema_length=0,
                    try_parse_dates=True
                )
            else:
                df = pl.read_csv(str(csv_file), separator=';')
            
            if "LEAD ID" in df.columns:
                if df_blaster is None:
                    df_blaster = df
                else:
                    df_blaster = pl.concat([df_blaster, df], how="diagonal")
            
            elif "cant_servicios" in df.columns:
                if df_assignment is None:
                    df_assignment = df
                else:
                    df_assignment = pl.concat([df_assignment, df], how="diagonal")
                
        except Exception as e:
            print(f"⚠️ Error leyendo {csv_file.name}: {str(e)[:100]}...")
    
    if df_blaster is None:
        print("❌ No se encontraron archivos blaster con columna 'LEAD ID'")
        return
    
    if df_assignment is None:
        print("❌ No se encontraron archivos assignment con columna 'cant_servicios'")
        return
    
    df_blaster = df_blaster.rename({col: col.strip() for col in df_blaster.columns})
    df_assignment = df_assignment.rename({col: col.strip() for col in df_assignment.columns})
    
    if 'IDENTIFICACION' in df_blaster.columns:
        df_blaster = df_blaster.rename({'IDENTIFICACION': 'identificacion'})
        df_blaster = df_blaster.with_columns(pl.col('identificacion').cast(pl.Utf8))
    elif 'identificacion' in df_blaster.columns:
        df_blaster = df_blaster.with_columns(pl.col('identificacion').cast(pl.Utf8))
    elif 'IDENTI' in df_blaster.columns:
        df_blaster = df_blaster.rename({'IDENTI': 'identificacion'})
        df_blaster = df_blaster.with_columns(pl.col('identificacion').cast(pl.Utf8))
    
    if 'cuenta' in df_assignment.columns:
        df_assignment = df_assignment.with_columns(
            pl.col('cuenta')
            .cast(pl.Utf8)
            .str.replace_all("-", "")
            .alias('cuenta')
        )
    
    if 'ESTADO' in df_blaster.columns:
        df_blaster = df_blaster.with_columns(
            pl.col('ESTADO').map_elements(translate_estado, return_dtype=pl.Utf8).alias('ESTADO')
        )
    
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
    
    if 'ESTADO' in cruzado.columns:
        efectivo_vigente = cruzado.filter(pl.col('ESTADO') != 'CONTESTADA')
    else:
        efectivo_vigente = cruzado
    
    if no_cruzado.height > 0:
        retiradas_columns = {k: v for k, v in COLUMNS_RETIRADAS.items() if k in no_cruzado.columns}
        
        retiradas = no_cruzado.select([
            pl.col(old_name).alias(new_name) for old_name, new_name in retiradas_columns.items()
        ])
        
        if 'cuenta_next' in retiradas.columns:
            retiradas = retiradas.with_columns(
                pl.col('cuenta_next').str.replace_all('"', '') + "-"
            )
        
        retiradas_path = output_path / f'cuentas_retiradas_blaster_{timestamp}.csv'
        retiradas.write_csv(retiradas_path, separator=';')
        print(f"✅ Guardado: {retiradas_path}")
    
    if efectivo_vigente.height > 0:
        efectiva_columns = {k: v for k, v in COLUMNS_EFECTIVA.items() if k in efectivo_vigente.columns}
        
        base_efectiva = efectivo_vigente.select([
            pl.col(old_name).alias(new_name) for old_name, new_name in efectiva_columns.items()
        ])
        
        if 'cuenta_next' in base_efectiva.columns:
            base_efectiva = base_efectiva.with_columns(
                pl.col('cuenta_next').str.replace_all('"', '') + "-"
            )
        
        efectiva_path = output_path / f'base_efectiva_vigente_{timestamp}.csv'
        base_efectiva.write_csv(efectiva_path, separator=';')
        print(f"✅ Guardado: {efectiva_path}")
    
    if cruzado.height > 0:
        blaster_columns = {k: v for k, v in COLUMNS_BLASTER_DEPURADO.items() if k in cruzado.columns}
        
        blaster_depurado = cruzado.select([
            pl.col(old_name).str.replace_all('"', '').alias(new_name) 
            if old_name == 'identificacion' 
            else pl.col(old_name).alias(new_name) 
            for old_name, new_name in blaster_columns.items()
        ])
        
        blaster_path = output_path / f'blaster_depurado_cargue_{timestamp}.csv'
        blaster_depurado.write_csv(blaster_path, separator=';')
        print(f"✅ Guardado: {blaster_path}")
    
    print(f"\n🎯 Proceso completado. Archivos guardados en: {output_path}")