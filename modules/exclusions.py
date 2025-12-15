import os
from datetime import datetime
import pandas as pd

def process_xlsx_file(input_file, output_path):
    
    output_path = f"{output_path}/"
    DataFrame = pd.read_excel(input_file, sheet_name="EXCLUSIONES", dtype=str)
    
    Columns_DF = ['DOCUMENTO', 'CUENTA', 'MINS', 'EMAILS']
    Characters = [' ', '<', '>']
    
    for column in Columns_DF:
        
        for char in Characters:
            DataFrame[column] = DataFrame[column].str.replace(char, '', regex=True)
        
        if column in ['DOCUMENTO', 'CUENTA', 'MINS']:
            DataFrame[column] = DataFrame[column].str.replace(r'[^0-9]', '', regex=True)
    
    DataFrame1 = DataFrame[DataFrame['MINS'].notna() & (DataFrame['MINS'] != '')]
    DataFrame2 = DataFrame[DataFrame['DOCUMENTO'].notna() & (DataFrame['DOCUMENTO'] != '')]
    DataFrame3 = DataFrame[DataFrame['CUENTA'].notna() & (DataFrame['CUENTA'] != '')]
    DataFrame4 = DataFrame[DataFrame['EMAILS'].notna() & (DataFrame['EMAILS'] != '')]
    
    Time_Value = datetime.now().strftime("%Y-%m-%d")
    
    output_path = f"{output_path}---- Bases para CARGUE ----"
    
    if output_path not in os.listdir():
        os.makedirs(output_path, exist_ok=True)  # Ensure the directory exists
    
    DataFrame2[['DOCUMENTO']].drop_duplicates().to_csv(f"{output_path}/Exclusion Documentos {Time_Value}.csv", index=False, header=True)
    DataFrame3[['CUENTA']].drop_duplicates().to_csv(f"{output_path}/Exclusion Cuentas {Time_Value}.csv", index=False, header=True)
    DataFrame1[['MINS']].drop_duplicates().to_csv(f"{output_path}/Exclusion Numeros {Time_Value}.csv", index=False, header=True)
    DataFrame4[['EMAILS']].drop_duplicates().to_csv(f"{output_path}/Exclusion Correos {Time_Value}.csv", index=False, header=True)