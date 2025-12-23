#Validaciones
import pandas as pd

def realizar_validaciones_cruzadas(df_pacientes, df_citas):
    """Realiza validaciones cruzadas entre tablas"""
    print("\n" + "="*60)
    print("VALIDACIONES CRUZADAS")
    print("="*60)
    
    # 1. Pacientes con citas vs pacientes sin citas
    pacientes_con_citas = df_citas['id_paciente'].unique()
    pacientes_sin_citas = set(df_pacientes['id_paciente']) - set(pacientes_con_citas)
    
    print(f"\n1. Pacientes sin citas médicas: {len(pacientes_sin_citas)}")
    
    # 2. Validar que todas las citas tengan pacientes válidos
    citas_con_paciente_invalido = df_citas[~df_citas['id_paciente'].isin(df_pacientes['id_paciente'])]
    print(f"\n2. Citas con pacientes no encontrados: {len(citas_con_paciente_invalido)}")
    
    if len(citas_con_paciente_invalido) > 0:
        print("   IDs de pacientes inválidos:")
        print(citas_con_paciente_invalido['id_paciente'].unique()[:10])
    
    # 3. Análisis de completitud por ciudad
    print("\n3. Completitud de datos por ciudad:")
    completitud_por_ciudad = df_pacientes.groupby('ciudad').agg({
        'telefono': lambda x: (x.notnull().sum() / len(x) * 100),
        'email': lambda x: (x.notnull().sum() / len(x) * 100),
        'sexo': lambda x: (x.notnull().sum() / len(x) * 100)
    }).round(2)
    
    print(completitud_por_ciudad)
    
    # 4. Costos por especialidad y médico
    print("\n4. Análisis de costos por especialidad:")
    costos_por_especialidad = df_citas.groupby('especialidad').agg({
        'costo': ['count', 'mean', 'min', 'max']
    }).round(2)
    
    print(costos_por_especialidad)
    
    # 5. Detectar outliers en costos
    print("\n5. Posibles outliers en costos:")
    for especialidad in df_citas['especialidad'].unique():
        subset = df_citas[df_citas['especialidad'] == especialidad]['costo']
        if len(subset) > 10:  # Solo si hay suficientes datos
            q1 = subset.quantile(0.25)
            q3 = subset.quantile(0.75)
            iqr = q3 - q1
            outliers = subset[(subset < q1 - 1.5 * iqr) | (subset > q3 + 1.5 * iqr)]
            if len(outliers) > 0:
                print(f"   {especialidad}: {len(outliers)} outliers detectados")
    
    # 6. Validar fechas lógicas (citas no en fines de semana, fechas coherentes)
    print("\n6. Validación de fechas:")
    
    # Citas en fines de semana
    df_citas['dia_semana'] = df_citas['fecha_cita'].dt.day_name()
    citas_fin_semana = df_citas[df_citas['dia_semana'].isin(['Saturday', 'Sunday'])]
    print(f"   Citas en fin de semana: {len(citas_fin_semana)}")
    
    # Citas muy antiguas (antes de 2000)
    citas_antiguas = df_citas[df_citas['fecha_cita'].dt.year < 2000]
    print(f"   Citas anteriores al año 2000: {len(citas_antiguas)}")
    
    return {
        'pacientes_sin_citas': list(pacientes_sin_citas),
        'citas_pacientes_invalidos': len(citas_con_paciente_invalido),
        'completitud_ciudad': completitud_por_ciudad,
        'citas_fin_semana': len(citas_fin_semana)
    }

# Ejecutar validaciones
print("Cargando datos limpios...")
df_pacientes = pd.read_csv('datoscln/pacientes_limpios.csv')
df_citas = pd.read_csv('datoscln/citas_limpios.csv')
df_citas['fecha_cita'] = pd.to_datetime(df_citas['fecha_cita'])

resultados = realizar_validaciones_cruzadas(df_pacientes, df_citas)
