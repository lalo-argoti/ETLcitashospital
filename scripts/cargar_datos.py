#exploratorio.py
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


