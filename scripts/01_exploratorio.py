import json
import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Función para cargar datos optimizada para memoria
def cargar_datos_optimizado(archivo_json, chunk_size=1000):
    """Carga datos en chunks para ahorrar memoria"""
    print(f"Cargando {archivo_json}...")
    
    with open(archivo_json, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Separar las tablas
    df_pacientes = pd.DataFrame(data['pacientes'])
    df_citas = pd.DataFrame(data['citas_medicas'])
    
    print(f"Pacientes: {df_pacientes.shape[0]} registros")
    print(f"Citas: {df_citas.shape[0]} registros")
    
    return df_pacientes, df_citas

# Cargar datos
df_pacientes, df_citas = cargar_datos_optimizado('dataset_hospital.json')

# Análisis básico de pacientes
print("\n" + "="*60)
print("ANÁLISIS DE PACIENTES")
print("="*60)
print("\n1. Información general:")
print(df_pacientes.info())

print("\n2. Valores nulos por columna:")
print(df_pacientes.isnull().sum())

print("\n3. Valores únicos por columna:")
for col in df_pacientes.columns:
    print(f"{col}: {df_pacientes[col].nunique()} valores únicos")

print("\n4. Estadísticas descriptivas:")
print(df_pacientes.describe(include='all'))

# Análisis básico de citas
print("\n" + "="*60)
print("ANÁLISIS DE CITAS MÉDICAS")
print("="*60)
print("\n1. Información general:")
print(df_citas.info())

print("\n2. Valores nulos por columna:")
print(df_citas.isnull().sum())

print("\n3. Estadísticas por especialidad:")
print(df_citas['especialidad'].value_counts())
