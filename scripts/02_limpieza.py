import pandas as pd
import numpy as np
from datetime import datetime, date
import re
from cargar_datos import cargar_datos_optimizado as cargar_datos
class DataCleaner:
    def __init__(self, df_pacientes, df_citas):
        self.df_pacientes = df_pacientes.copy()
        self.df_citas = df_citas.copy()
        self.metrics = {
            'antes': {},
            'despues': {}
        }
    
    
    
    def calcular_metricas_iniciales(self):
        """Calcula métricas antes de la limpieza"""
        self.metrics['antes']['pacientes'] = {
            'total': len(self.df_pacientes),
            'nulos_edad': self.df_pacientes['edad'].isnull().sum(),
            'nulos_sexo': self.df_pacientes['sexo'].isnull().sum(),
            'nulos_email': self.df_pacientes['email'].isnull().sum(),
            'nulos_telefono': self.df_pacientes['telefono'].isnull().sum(),
            'inconsistencias_edad': self._contar_inconsistencias_edad()
        }
        
        self.metrics['antes']['citas'] = {
            'total': len(self.df_citas),
            'nulos_fecha': self.df_citas['fecha_cita'].isnull().sum(),
            'nulos_costo': self.df_citas['costo'].isnull().sum(),
            'nulos_medico': self.df_citas['medico'].isnull().sum(),
            'nulos_estado': self.df_citas['estado_cita'].isnull().sum(),
            'fechas_invalidas': self._contar_fechas_invalidas()
        }
    
    def _contar_inconsistencias_edad(self):
        """Cuenta inconsistencias entre edad y fecha_nacimiento"""
        count = 0
        for idx, row in self.df_pacientes.iterrows():
            if pd.notnull(row['edad']) and pd.notnull(row['fecha_nacimiento']):
                try:
                    fecha_nac = pd.to_datetime(row['fecha_nacimiento'])
                    edad_calculada = date.today().year - fecha_nac.year
                    if abs(edad_calculada - row['edad']) > 1:
                        count += 1
                except:
                    count += 1
        return count
    
    def _contar_fechas_invalidas(self):
        """Cuenta fechas inválidas"""
        count = 0
        for fecha in self.df_citas['fecha_cita']:
            if pd.notnull(fecha):
                try:
                    pd.to_datetime(fecha, format='%Y-%m-%d', errors='raise')
                except:
                    count += 1
        return count
    
    def limpiar_pacientes(self):
        """Aplica limpieza a la tabla de pacientes"""
        print("Limpiando tabla de pacientes...")
        
        # 1. Normalizar sexo
        self.df_pacientes['sexo'] = self.df_pacientes['sexo'].apply(
            lambda x: 'F' if str(x).strip().upper() in ['F', 'FEMALE'] 
                      else 'M' if str(x).strip().upper() in ['M', 'MALE']
                      else np.nan
        )
        
        # 2. Calcular edad desde fecha_nacimiento (priorizar sobre edad existente)
        def calcular_edad_desde_fecha(fecha_str, edad_actual):
            if pd.isnull(fecha_str):
                return edad_actual
            
            try:
                fecha_nac = pd.to_datetime(fecha_str, errors='coerce')
                if pd.isnull(fecha_nac):
                    return edad_actual
                
                hoy = date.today()
                edad_calculada = hoy.year - fecha_nac.year
                
                # Ajustar si aún no ha cumplido años este año
                if (hoy.month, hoy.day) < (fecha_nac.month, fecha_nac.day):
                    edad_calculada -= 1
                
                return int(edad_calculada)
            except:
                return edad_actual
        
        self.df_pacientes['edad'] = self.df_pacientes.apply(
            lambda row: calcular_edad_desde_fecha(row['fecha_nacimiento'], row['edad']),
            axis=1
        )
        
        # 3. Limpiar teléfonos (quitar guiones, espacios, etc.)
        def limpiar_telefono(tel):
            if pd.isnull(tel):
                return np.nan
            # Quitar todo excepto números
            tel_limpio = re.sub(r'[^\d]', '', str(tel))
            # Validar que tenga entre 7 y 15 dígitos
            if 7 <= len(tel_limpio) <= 15:
                return tel_limpio
            return np.nan
        
        self.df_pacientes['telefono'] = self.df_pacientes['telefono'].apply(limpiar_telefono)
        
        # 4. Validar emails
        def validar_email(email):
            if pd.isnull(email):
                return np.nan
            email = str(email).strip().lower()
            patron = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            if re.match(patron, email):
                return email
            return np.nan
        
        self.df_pacientes['email'] = self.df_pacientes['email'].apply(validar_email)
        
        # 5. Normalizar nombres (capitalizar correctamente)
        def normalizar_nombre(nombre):
            if pd.isnull(nombre):
                return np.nan
            # Capitalizar cada palabra
            return ' '.join([word.capitalize() for word in str(nombre).split()])
        
        self.df_pacientes['nombre'] = self.df_pacientes['nombre'].apply(normalizar_nombre)
        
        # 6. Crear columna de calidad
        def calcular_score_calidad(row):
            score = 0
            if pd.notnull(row['nombre']):
                score += 20
            if pd.notnull(row['edad']):
                score += 20
            if pd.notnull(row['sexo']):
                score += 20
            if pd.notnull(row['telefono']):
                score += 20
            if pd.notnull(row['email']):
                score += 20
            return score
        
        self.df_pacientes['score_calidad'] = self.df_pacientes.apply(calcular_score_calidad, axis=1)
        
        print(f"  → Total pacientes después: {len(self.df_pacientes)}")
    
    def limpiar_citas(self):
        """Aplica limpieza a la tabla de citas"""
        print("Limpiando tabla de citas...")
        
        # 1. Limpiar fechas inválidas
        def corregir_fecha_inteligente(fecha_str):
            """
            Corrige fechas con formato YYYY-DD-MM a YYYY-MM-DD
            y detecta fechas ambiguas
            """
            if pd.isna(fecha_str) or str(fecha_str).strip() == '':
                return pd.NaT, False, 'NULO'
            
            fecha_str = str(fecha_str).strip()
            original = fecha_str
            
            # 1. Intentar parseo directo primero
            try:
                fecha = pd.to_datetime(fecha_str, format='%Y-%m-%d', errors='raise')
                return fecha, False, 'VÁLIDA'
            except:
                pass
            
            # 2. Separar componentes
            import re
            partes = re.split(r'[-/\.]', fecha_str)
            if len(partes) != 3:
                return pd.NaT, False, 'FORMATO_INVALIDO'
            
            try:
                año = int(partes[0])
                comp1 = int(partes[1])  # Posible mes o día
                comp2 = int(partes[2])  # Posible día o mes
                
                # REGLA 1: Si comp1 > 12 y comp2 ≤ 12 → Formato YYYY-DD-MM
                if comp1 > 12 and comp2 <= 12:
                    # Formato era YYYY-DD-MM, corregir a YYYY-MM-DD
                    mes = comp2
                    dia = min(comp1, 31)  # Limitar día a 31
                    try:
                        fecha = pd.Timestamp(f"{año}-{mes:02d}-{dia:02d}")
                        return fecha, False, f'CORREGIDO_DDMM({comp1}→{dia})'
                    except:
                        return pd.NaT, False, 'ERROR_CORRECCION'
                
                # REGLA 2: Si comp1 ≤ 12 y comp2 > 12 → Formato YYYY-MM-DD (correcto)
                elif comp1 <= 12 and comp2 > 12:
                    mes = comp1
                    dia = min(comp2, 31)
                    try:
                        fecha = pd.Timestamp(f"{año}-{mes:02d}-{dia:02d}")
                        return fecha, False, 'VÁLIDA_MMDD'
                    except:
                        return pd.NaT, False, 'ERROR_VALIDACION'
                
                # REGLA 3: Ambos ≤ 12 → AMBIGUO (ej: 2025-05-07)
                elif comp1 <= 12 and comp2 <= 12:
                    # Heurística: asumir que es YYYY-MM-DD (formato más común)
                    mes = comp1
                    dia = min(comp2, 31)
                    try:
                        fecha = pd.Timestamp(f"{año}-{mes:02d}-{dia:02d}")
                        return fecha, True, f'AMBIGUA_ASUMIDO_MMDD({comp1}-{comp2})'
                    except:
                        return pd.NaT, False, 'AMBIGUA_NO_VALIDA'
                
                # REGLA 4: Ambos > 31 o algún error
                else:
                    return pd.NaT, False, 'COMPONENTES_INVALIDOS'
                    
            except Exception as e:
                return pd.NaT, False, f'ERROR: {str(e)}'
        ##########################################################################        
                
        # ============================================
        # APLICAR CORRECCIÓN A TODAS LAS FECHAS
        # ============================================
        print("  [Paso 1] Aplicando corrección inteligente de fechas...")
        
        resultados = []
        for idx, row in self.df_citas.iterrows():
            fecha_corregida, es_ambigua, motivo = corregir_fecha_inteligente(row['fecha_cita'])
            resultados.append({
                'fecha_corregida': fecha_corregida,
                'es_ambigua': es_ambigua,
                'motivo_correccion': motivo
            })
        
        # Agregar columnas nuevas
        self.df_citas['fecha_cita_original'] = self.df_citas['fecha_cita'].copy()
        self.df_citas['fecha_cita'] = [r['fecha_corregida'] for r in resultados]
        self.df_citas['fecha_ambigua'] = [r['es_ambigua'] for r in resultados]
        self.df_citas['motivo_correccion'] = [r['motivo_correccion'] for r in resultados]
        
        # Estadísticas de corrección
        total_correcciones = sum(1 for r in resultados if 'CORREGIDO' in r['motivo_correccion'])
        total_ambiguas = sum(r['es_ambigua'] for r in resultados)
        
        print(f"    • Correcciones aplicadas: {total_correcciones}")
        print(f"    • Fechas ambiguas detectadas: {total_ambiguas}")
        # 2. Validar que fecha_cita no sea futura lejana (máximo 2 años)
        hoy = pd.Timestamp.now()
        max_futuro = hoy + pd.DateOffset(years=2)
        self.df_citas['fecha_cita'] = self.df_citas['fecha_cita'].apply(
            lambda x: x if pd.isnull(x) or x <= max_futuro else np.nan
        )
        
        # 3. Rellenar costos nulos con promedios por especialidad
        costo_promedio = self.df_citas.groupby('especialidad')['costo'].median()
        
        def completar_costo(row):
            if pd.isnull(row['costo']) and pd.notnull(row['especialidad']):
                especialidad = row['especialidad']
                if especialidad in costo_promedio:
                    return costo_promedio[especialidad]
            return row['costo']
        
        self.df_citas['costo'] = self.df_citas.apply(completar_costo, axis=1)
        
        # 4. Establecer estado por defecto para citas sin estado
        def determinar_estado(row):
            if pd.isnull(row['estado_cita']):
                if pd.isnull(row['fecha_cita']):
                    return 'No Programada'
                elif row['fecha_cita'] < hoy:
                    return 'Completada'
                else:
                    return 'Programada'
            return row['estado_cita']
        
        self.df_citas['estado_cita'] = self.df_citas.apply(determinar_estado, axis=1)
        
        # 5. Validar referencias a pacientes
        pacientes_validos = set(self.df_pacientes['id_paciente'].unique())
        self.df_citas['paciente_valido'] = self.df_citas['id_paciente'].isin(pacientes_validos)
        
        # 6. Crear columna de año-mes para análisis
        self.df_citas['anio_mes'] = self.df_citas['fecha_cita'].dt.to_period('M')
        
        print(f"  → Total citas después: {len(self.df_citas)}")
    
    def calcular_metricas_finales(self):
        """Calcula métricas después de la limpieza"""
        self.metrics['despues']['pacientes'] = {
            'total': len(self.df_pacientes),
            'nulos_edad': self.df_pacientes['edad'].isnull().sum(),
            'nulos_sexo': self.df_pacientes['sexo'].isnull().sum(),
            'nulos_email': self.df_pacientes['email'].isnull().sum(),
            'nulos_telefono': self.df_pacientes['telefono'].isnull().sum(),
            'score_calidad_promedio': self.df_pacientes['score_calidad'].mean()
        }
        
        self.metrics['despues']['citas'] = {
            'total': len(self.df_citas),
            'nulos_fecha': self.df_citas['fecha_cita'].isnull().sum(),
            'nulos_costo': self.df_citas['costo'].isnull().sum(),
            'nulos_medico': self.df_citas['medico'].isnull().sum(),
            'nulos_estado': self.df_citas['estado_cita'].isnull().sum(),
            'pacientes_invalidos': (~self.df_citas['paciente_valido']).sum()
        }
    
    def generar_reporte(self):
        """Genera reporte de métricas"""
        print("\n" + "="*60)
        print("REPORTE DE CALIDAD DE DATOS")
        print("="*60)
        
        print("\nPACIENTES - COMPARACIÓN:")
        print("-"*40)
        print(f"{'Métrica':<25} {'Antes':<10} {'Después':<10} {'Mejora':<10}")
        print("-"*40)
        
        for key in self.metrics['antes']['pacientes']:
            if key in self.metrics['despues']['pacientes']:
                antes = self.metrics['antes']['pacientes'][key]
                despues = self.metrics['despues']['pacientes'].get(key, 'N/A')
                mejora = ""
                if isinstance(antes, (int, float)) and isinstance(despues, (int, float)):
                    mejora = f"{(antes - despues) / max(antes, 1) * 100:.1f}%"
                print(f"{key:<25} {str(antes):<10} {str(despues):<10} {mejora:<10}")
        
        print("\nCITAS - COMPARACIÓN:")
        print("-"*40)
        print(f"{'Métrica':<25} {'Antes':<10} {'Despues':<10} {'Mejora':<10}")
        print("-"*40)
        
        for key in self.metrics['antes']['citas']:
            if key in self.metrics['despues']['citas']:
                antes = self.metrics['antes']['citas'][key]
                despues = self.metrics['despues']['citas'].get(key, 'N/A')
                mejora = ""
                if isinstance(antes, (int, float)) and isinstance(despues, (int, float)):
                    mejora = f"{(antes - despues) / max(antes, 1) * 100:.1f}%"
                print(f"{key:<25} {str(antes):<10} {str(despues):<10} {mejora:<10}")
    
    def exportar_datos_limpios(self):
        """Exporta los datos limpios a archivos"""
        print("\nExportando datos limpios...")
        
        # Exportar pacientes
        self.df_pacientes.to_csv('datoscln/pacientes_limpios.csv', index=False, encoding='utf-8')
        self.df_pacientes.to_parquet('datoscln/pacientes_limpios.parquet', index=False)
        
        # Exportar citas
        self.df_citas.to_csv('datoscln/citas_limpios.csv', index=False, encoding='utf-8')
        self.df_citas.to_parquet('datoscln/citas_limpios.parquet', index=False)
        
        print("✓ Datos exportados exitosamente:")
        print("  - datoscln/pacientes_limpios.csv")
        print("  - datoscln/pacientes_limpios.parquet")
        print("  - datoscln/citas_limpios.csv")
        print("  - datoscln/citas_limpios.parquet")
    
    def ejecutar_pipeline(self):
        """Ejecuta todo el pipeline de limpieza"""
        print("Iniciando pipeline de limpieza...")
        
        # 1. Calcular métricas iniciales
        self.calcular_metricas_iniciales()
        
        # 2. Aplicar limpieza
        self.limpiar_pacientes()
        self.limpiar_citas()
        
        # 3. Calcular métricas finales
        self.calcular_metricas_finales()
        
        # 4. Generar reporte
        self.generar_reporte()
        
        # 5. Exportar datos
        self.exportar_datos_limpios()
        
        return self.df_pacientes, self.df_citas

# Uso del pipeline
if __name__ == "__main__":
    # Cargar datos
    df_pacientes, df_citas = cargar_datos('dataset_hospital.json')
    
    # Ejecutar limpieza
    cleaner = DataCleaner(df_pacientes, df_citas)
    pacientes_limpios, citas_limpias = cleaner.ejecutar_pipeline()
    
    print("\n¡Proceso completado exitosamente!")
