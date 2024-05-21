import os
import pandas as pd

# Path to the folder containing the Excel files
folder_path = 'Transmisiones_test'

# Function to delete the first N rows of an Excel file
def delete_first_n_rows(file_path, n):
    df = pd.read_excel(file_path, header=None)  # Read without header
    df = df.iloc[n:]  # Drop the first n rows
    df.columns = df.iloc[0]  # Set the first row as header
    df = df[1:]  # Drop the header row
    df.to_excel(file_path, index=False)

# Function to convert an Excel file to CSV
def convert_to_csv(file_path, csv_path):
    df = pd.read_excel(file_path)  # Read the Excel file
    df.to_csv(csv_path, index=False)  # Convert to CSV

# Lists to store DataFrames
excedentes_list = []
requerimientos_list = []

# Loop through all the files in the folder
for file_name in os.listdir(folder_path):
    if file_name.endswith('.xlsx'):
        file_path = os.path.join(folder_path, file_name)
        if file_name.startswith('Requerimientos_'):
            delete_first_n_rows(file_path, 11)
        elif file_name.startswith('Excedentes_'):
            delete_first_n_rows(file_path, 8)
        
        # Define the path for the CSV file
        csv_file_name = file_name.replace('.xlsx', '.csv')
        csv_path = os.path.join(folder_path, csv_file_name)
        
        # Convert the Excel file to CSV
        convert_to_csv(file_path, csv_path)
        
        # Read the CSV file and append to the corresponding list
        if file_name.startswith('Excedentes_'):
            df = pd.read_csv(csv_path)
            excedentes_list.append(df)
        elif file_name.startswith('Requerimientos_'):
            df = pd.read_csv(csv_path)
            requerimientos_list.append(df)

print("Conversion completed.")

# Concatenate and sort DataFrames in each list
if excedentes_list:
    excedentes_combined = pd.concat(excedentes_list, ignore_index=True)
    excedentes_combined['FECHA TRANSMISIÓN'] = pd.to_datetime(excedentes_combined['FECHA TRANSMISIÓN'], format='%Y-%m-%d')
    excedentes_combined['HORA TRANSMISIÓN'] = pd.to_datetime(excedentes_combined['HORA TRANSMISIÓN'], format='%H:%M:%S').dt.time
    excedentes_combined = excedentes_combined.sort_values(by=['FECHA TRANSMISIÓN', 'HORA TRANSMISIÓN'])
    excedentes_combined.to_csv(os.path.join(folder_path, 'Excedentes_combined.csv'), index=False)

if requerimientos_list:
    requerimientos_combined = pd.concat(requerimientos_list, ignore_index=True)
    requerimientos_combined['Fecha de transmisión del material'] = pd.to_datetime(requerimientos_combined['Fecha de transmisión del material'], format='mixed')
    requerimientos_combined = requerimientos_combined.sort_values(by=['Fecha de transmisión del material', 'Hora transmisión'])
    requerimientos_combined.to_csv(os.path.join(folder_path, 'Requerimientos_combined.csv'), index=False)

print('Archivos combinados creados exitosamente en formato CSV, ordenados por fecha y hora.')

# File names of the combined CSV files
excedentes_combined_file = 'Excedentes_combined.csv'
requerimientos_combined_file = 'Requerimientos_combined.csv'

# Full paths to the combined CSV files
excedentes_combined_path = os.path.join(folder_path, excedentes_combined_file)
requerimientos_combined_path = os.path.join(folder_path, requerimientos_combined_file)

# Function to count the number of rows in a CSV file
def count_rows(file_path):
    df = pd.read_csv(file_path)
    return len(df)

# Function to count duplicate rows in a DataFrame
def count_duplicates(df):
    return df.duplicated().sum()

# Function to read a CSV file into a DataFrame
def read_csv_file(file_path):
    return pd.read_csv(file_path)

# Function to count duplicates based on specific columns between two DataFrames
def count_cross_duplicates(df1, df2, df1_columns, df2_columns):
    df1_subset = df1[df1_columns].copy()
    df2_subset = df2[df2_columns].copy()
    # Ensure the columns are of the same type (string)
    for col in df1_columns:
        df1_subset[col] = df1_subset[col].astype(str)
    for col in df2_columns:
        df2_subset[col] = df2_subset[col].astype(str)
    merged_df = pd.merge(df1_subset, df2_subset, left_on=df1_columns, right_on=df2_columns)
    return len(merged_df)

# Check if the combined files exist and count the rows
if os.path.exists(excedentes_combined_path):
    excedentes_row_count = count_rows(excedentes_combined_path)
    print(f'El archivo {excedentes_combined_file} tiene {excedentes_row_count} renglones.')

if os.path.exists(requerimientos_combined_path):
    requerimientos_row_count = count_rows(requerimientos_combined_path)
    print(f'El archivo {requerimientos_combined_file} tiene {requerimientos_row_count} renglones.')

# Read the files and count duplicates within each file
if os.path.exists(excedentes_combined_path):
    df_excedentes = read_csv_file(excedentes_combined_path)
    excedentes_duplicates = count_duplicates(df_excedentes)
    print(f'El archivo {excedentes_combined_file} tiene {excedentes_duplicates} registros repetidos dentro del archivo.')

if os.path.exists(requerimientos_combined_path):
    df_requerimientos = read_csv_file(requerimientos_combined_path)
    requerimientos_duplicates = count_duplicates(df_requerimientos)
    print(f'El archivo {requerimientos_combined_file} tiene {requerimientos_duplicates} registros repetidos dentro del archivo.')

# Check for duplicates between the two files based on specific columns
if os.path.exists(excedentes_combined_path) and os.path.exists(requerimientos_combined_path):
    cross_duplicates = count_cross_duplicates(
        df_excedentes, df_requerimientos,
        df1_columns=['ESTADO', 'FECHA TRANSMISIÓN', 'HORA TRANSMISIÓN'],
        df2_columns=['Entidad', 'Fecha de transmisión del material', 'Hora transmisión']
    )
    print(f'Entre los archivos {excedentes_combined_file} y {requerimientos_combined_file} hay {cross_duplicates} registros repetidos basados en las columnas especificadas.')

# Function to find and remove cross-duplicates from df2 based on df1
def remove_cross_duplicates(df1, df2, df1_columns, df2_columns):
    df1_subset = df1[df1_columns].copy()
    df2_subset = df2[df2_columns].copy()
    # Ensure the columns are of the same type (string)
    for col in df1_columns:
        df1_subset[col] = df1_subset[col].astype(str)
    for col in df2_columns:
        df2_subset[col] = df2_subset[col].astype(str)
    merged_df = pd.merge(df2, df1_subset, how='left', left_on=df2_columns, right_on=df1_columns, indicator=True)
    df2_cleaned = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])
    return df2_cleaned

# Check if the combined files exist and read them
if os.path.exists(excedentes_combined_path) and os.path.exists(requerimientos_combined_path):
    df_excedentes = read_csv_file(excedentes_combined_path)
    df_requerimientos = read_csv_file(requerimientos_combined_path)
    
    # Remove duplicates from requerimientos based on excedentes
    df_requerimientos_cleaned = remove_cross_duplicates(
        df_excedentes, df_requerimientos,
        df1_columns=['ESTADO', 'FECHA TRANSMISIÓN', 'HORA TRANSMISIÓN'],
        df2_columns=['Entidad', 'Fecha de transmisión del material', 'Hora transmisión']
    )
    
    # Save the cleaned DataFrame to a new CSV file or overwrite the existing one
    df_requerimientos_cleaned.to_csv(requerimientos_combined_path, index=False)
    print(f'Se han eliminado los registros duplicados del archivo {requerimientos_combined_file}.')
else:
    print(f'No se encontraron uno o ambos archivos: {excedentes_combined_file}, {requerimientos_combined_file}.')

# Function to rename columns in a DataFrame
def rename_columns(df, columns_mapping):
    return df.rename(columns=columns_mapping)

# Function to standardize date and time formats
def standardize_datetime(df, date_col, time_col):
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce').dt.strftime('%Y-%m-%d')
    df[time_col] = pd.to_datetime(df[time_col], errors='coerce').dt.strftime('%H:%M:%S')
    return df

# Column mapping for renaming
columns_mapping = {
    'Entidad': 'ESTADO',
    'Emisora': 'EMISORA',
    'Versión': 'VERSIÓN',
    'Folio': 'MATERIAL',
    'Actor': 'ACTOR',
    'Fecha de transmisión del material': 'FECHA TRANSMISIÓN',
    'Hora transmisión': 'HORA TRANSMISIÓN'
}

# Check if the combined files exist and read them
if os.path.exists(excedentes_combined_path) and os.path.exists(requerimientos_combined_path):
    df_excedentes = read_csv_file(excedentes_combined_path)
    df_requerimientos = read_csv_file(requerimientos_combined_path)
    
    # Rename columns in df_requerimientos
    df_requerimientos_renamed = rename_columns(df_requerimientos, columns_mapping)
    
    # Remove duplicate columns before any further processing
    df_excedentes = df_excedentes.loc[:, ~df_excedentes.columns.duplicated()]
    df_requerimientos_renamed = df_requerimientos_renamed.loc[:, ~df_requerimientos_renamed.columns.duplicated()]
    
    # Standardize date and time formats
    df_excedentes = standardize_datetime(df_excedentes, 'FECHA TRANSMISIÓN', 'HORA TRANSMISIÓN')
    df_requerimientos_renamed = standardize_datetime(df_requerimientos_renamed, 'FECHA TRANSMISIÓN', 'HORA TRANSMISIÓN')
    
    # Reset index for both DataFrames to avoid InvalidIndexError
    df_excedentes.reset_index(drop=True, inplace=True)
    df_requerimientos_renamed.reset_index(drop=True, inplace=True)
    
    # Combine the DataFrames with outer join to include all columns
    combined_df = pd.concat([df_excedentes, df_requerimientos_renamed], ignore_index=True, sort=False, join='outer')
    
    # Save the combined DataFrame to a new CSV file
    combined_file_path = os.path.join(folder_path, 'Transmisiones.csv')
    combined_df.to_csv(combined_file_path, index=False)
    print(f'Se han combinado y guardado los archivos en {combined_file_path}.')
else:
    print(f'No se encontraron uno o ambos archivos: {excedentes_combined_file}, {requerimientos_combined_file}.')