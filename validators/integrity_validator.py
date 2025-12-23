"""
Validador de integridad de datos para datasets hospitalarios
"""
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple, Optional


class IntegrityValidator:
    """Validador de integridad referencial y consistencia de datos"""
    
    def validate_foreign_keys(self, 
                            child_df: pd.DataFrame, 
                            child_column: str,
                            parent_df: pd.DataFrame, 
                            parent_column: str) -> Dict[str, Any]:
        """
        Valida que todos los valores en child_column existan en parent_column
        
        Returns:
            Dict con resultados de la validación
        """
        if child_column not in child_df.columns:
            return {
                'passed': False,
                'message': f'Columna {child_column} no encontrada en DataFrame hijo',
                'invalid_references': 0,
                'invalid_ids': []
            }
        
        if parent_column not in parent_df.columns:
            return {
                'passed': False,
                'message': f'Columna {parent_column} no encontrada en DataFrame padre',
                'invalid_references': 0,
                'invalid_ids': []
            }
        
        # Encontrar referencias inválidas
        child_values = set(child_df[child_column].dropna().unique())
        parent_values = set(parent_df[parent_column].dropna().unique())
        
        invalid_references = child_values - parent_values
        invalid_count = len(invalid_references)
        
        return {
            'passed': invalid_count == 0,
            'message': f'{invalid_count} referencias inválidas encontradas' if invalid_count > 0 else 'Todas las referencias son válidas',
            'invalid_references': invalid_count,
            'invalid_ids': list(invalid_references)
        }
    
    def validate_date_coherence(self, 
                              df: pd.DataFrame, 
                              date_column: str,
                              max_future_days: int = 365*2) -> Dict[str, Any]:
        """
        Valida que las fechas sean coherentes (no futuras extremas ni pasadas extremas)
        
        Args:
            df: DataFrame a validar
            date_column: Nombre de la columna de fecha
            max_future_days: Máximo número de días en el futuro permitido
            
        Returns:
            Dict con resultados de la validación
        """
        if date_column not in df.columns:
            return {
                'passed': False,
                'message': f'Columna {date_column} no encontrada',
                'future_dates': 0,
                'invalid_dates': 0
            }
        
        today = pd.Timestamp.now().normalize()
        max_future_date = today + timedelta(days=max_future_days)
        
        # Convertir a datetime si es necesario
        date_series = pd.to_datetime(df[date_column], errors='coerce')
        
        # Contar fechas futuras extremas
        future_mask = date_series > max_future_date
        future_count = future_mask.sum()
        
        # Contar fechas inválidas (NaT)
        invalid_count = date_series.isna().sum()
        
        # Encontrar fechas problemáticas
        problematic_dates = []
        if future_count > 0:
            problematic_dates = df.loc[future_mask, date_column].tolist()
        
        return {
            'passed': (future_count == 0) and (invalid_count == 0),
            'message': f'{future_count} fechas futuras extremas, {invalid_count} fechas inválidas',
            'future_dates': int(future_count),
            'invalid_dates': int(invalid_count),
            'problematic_dates': problematic_dates,
            'total_records': len(df)
        }
    
    def validate_column_values(self, 
                             df: pd.DataFrame, 
                             column: str,
                             valid_values: List[str]) -> Dict[str, Any]:
        """
        Valida que los valores de una columna estén en la lista de valores permitidos
        
        Returns:
            Dict con resultados de la validación
        """
        if column not in df.columns:
            return {
                'passed': False,
                'message': f'Columna {column} no encontrada',
                'invalid_count': 0,
                'invalid_values': []
            }
        
        # Contar valores inválidos (nulos o no en lista)
        invalid_mask = ~df[column].isin(valid_values)
        invalid_mask = invalid_mask | df[column].isna()
        invalid_count = invalid_mask.sum()
        
        invalid_values = []
        if invalid_count > 0:
            invalid_values = df.loc[invalid_mask, column].unique().tolist()
        
        return {
            'passed': invalid_count == 0,
            'message': f'{invalid_count} valores inválidos en columna {column}',
            'invalid_count': int(invalid_count),
            'invalid_values': invalid_values,
            'valid_values_count': len(df) - invalid_count
        }
    
    def validate_numeric_range(self,
                             df: pd.DataFrame,
                             column: str,
                             min_val: Optional[float] = None,
                             max_val: Optional[float] = None) -> Dict[str, Any]:
        """
        Valida que los valores numéricos estén dentro de un rango
        
        Returns:
            Dict con resultados de la validación
        """
        if column not in df.columns:
            return {
                'passed': False,
                'message': f'Columna {column} no encontrada',
                'out_of_range': 0
            }
        
        # Convertir a numérico
        numeric_series = pd.to_numeric(df[column], errors='coerce')
        
        # Aplicar máscaras
        out_of_range_mask = pd.Series(False, index=df.index)
        
        if min_val is not None:
            out_of_range_mask = out_of_range_mask | (numeric_series < min_val)
        
        if max_val is not None:
            out_of_range_mask = out_of_range_mask | (numeric_series > max_val)
        
        out_of_range_count = out_of_range_mask.sum()
        
        return {
            'passed': out_of_range_count == 0,
            'message': f'{out_of_range_count} valores fuera de rango en {column}',
            'out_of_range': int(out_of_range_count),
            'min_val': min_val,
            'max_val': max_val
        }
    
    def validate_all_integrity(self,
                             pacientes_df: pd.DataFrame,
                             citas_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Valida la integridad completa del dataset hospitalario
        
        Args:
            pacientes_df: DataFrame de pacientes limpio
            citas_df: DataFrame de citas limpio
            
        Returns:
            Dict con todos los resultados de validación
        """
        resultados = {
            'overall_passed': True,
            'tests': {},
            'summary': {
                'total_tests': 0,
                'passed_tests': 0,
                'failed_tests': 0
            }
        }
        
        # 1. Validar integridad referencial
        ref_result = self.validate_foreign_keys(
            citas_df, 'id_paciente',
            pacientes_df, 'id_paciente'
        )
        resultados['tests']['referencial_pacientes'] = ref_result
        resultados['summary']['total_tests'] += 1
        if ref_result['passed']:
            resultados['summary']['passed_tests'] += 1
        else:
            resultados['summary']['failed_tests'] += 1
            resultados['overall_passed'] = False
        
        # 2. Validar fechas de citas coherentes
        fecha_result = self.validate_date_coherence(
            citas_df, 'fecha_cita', max_future_days=365*2
        )
        resultados['tests']['coherencia_fechas'] = fecha_result
        resultados['summary']['total_tests'] += 1
        if fecha_result['passed']:
            resultados['summary']['passed_tests'] += 1
        else:
            resultados['summary']['failed_tests'] += 1
            resultados['overall_passed'] = False
        
        # 3. Validar estados de cita válidos
        estados_validos = ['Programada', 'Completada', 'Cancelada', 
                         'Reprogramada', 'No Programada']
        estado_result = self.validate_column_values(
            citas_df, 'estado_cita', estados_validos
        )
        resultados['tests']['estados_cita'] = estado_result
        resultados['summary']['total_tests'] += 1
        if estado_result['passed']:
            resultados['summary']['passed_tests'] += 1
        else:
            resultados['summary']['failed_tests'] += 1
        
        # 4. Validar especialidades válidas
        especialidades_validas = ['Cardiología', 'Neurología', 'Pediatría',
                                'Ginecología', 'Ortopedia']
        especialidad_result = self.validate_column_values(
            citas_df, 'especialidad', especialidades_validas
        )
        resultados['tests']['especialidades'] = especialidad_result
        resultados['summary']['total_tests'] += 1
        if especialidad_result['passed']:
            resultados['summary']['passed_tests'] += 1
        else:
            resultados['summary']['failed_tests'] += 1
        
        # 5. Validar rangos de edad
        if 'edad' in pacientes_df.columns:
            edad_result = self.validate_numeric_range(
                pacientes_df, 'edad', min_val=0, max_val=120
            )
            resultados['tests']['rango_edad'] = edad_result
            resultados['summary']['total_tests'] += 1
            if edad_result['passed']:
                resultados['summary']['passed_tests'] += 1
            else:
                resultados['summary']['failed_tests'] += 1
        
        # 6. Validar unicidad de IDs
        resultados['tests']['unicidad_ids'] = {}
        
        # IDs de pacientes únicos
        if 'id_paciente' in pacientes_df.columns:
            dup_pacientes = pacientes_df.duplicated(subset=['id_paciente']).sum()
            pacientes_unico = dup_pacientes == 0
            resultados['tests']['unicidad_ids']['pacientes'] = {
                'passed': pacientes_unico,
                'duplicates': int(dup_pacientes),
                'message': f'{dup_pacientes} IDs de pacientes duplicados' if dup_pacientes > 0 else 'IDs únicos'
            }
            resultados['summary']['total_tests'] += 1
            if pacientes_unico:
                resultados['summary']['passed_tests'] += 1
            else:
                resultados['summary']['failed_tests'] += 1
        
        # IDs de citas únicos
        if 'id_cita' in citas_df.columns:
            dup_citas = citas_df.duplicated(subset=['id_cita']).sum()
            citas_unico = dup_citas == 0
            resultados['tests']['unicidad_ids']['citas'] = {
                'passed': citas_unico,
                'duplicates': int(dup_citas),
                'message': f'{dup_citas} IDs de citas duplicados' if dup_citas > 0 else 'IDs únicos'
            }
            resultados['summary']['total_tests'] += 1
            if citas_unico:
                resultados['summary']['passed_tests'] += 1
            else:
                resultados['summary']['failed_tests'] += 1
        
        return resultados
    
    def generate_integrity_report(self, resultados: Dict[str, Any]) -> str:
        """
        Genera un reporte legible de los resultados de integridad
        
        Returns:
            String con reporte formateado
        """
        report_lines = []
        report_lines.append("=" * 60)
        report_lines.append("REPORTE DE INTEGRIDAD DE DATOS HOSPITALARIOS")
        report_lines.append("=" * 60)
        
        # Resumen
        summary = resultados['summary']
        report_lines.append(f"\n📊 RESUMEN:")
        report_lines.append(f"   Total pruebas: {summary['total_tests']}")
        report_lines.append(f"   Pruebas exitosas: {summary['passed_tests']}")
        report_lines.append(f"   Pruebas fallidas: {summary['failed_tests']}")
        
        overall_status = "✅ PASÓ" if resultados['overall_passed'] else "❌ FALLÓ"
        report_lines.append(f"   Estado general: {overall_status}")
        
        # Detalles por prueba
        report_lines.append(f"\n🔍 DETALLES POR PRUEBA:")
        for test_name, result in resultados['tests'].items():
            if isinstance(result, dict) and 'passed' in result:
                status = "✓" if result['passed'] else "✗"
                message = result.get('message', 'Sin mensaje')
                report_lines.append(f"   {status} {test_name}: {message}")
            elif isinstance(result, dict):  # Para unicidad_ids que tiene subniveles
                for sub_test, sub_result in result.items():
                    if 'passed' in sub_result:
                        status = "✓" if sub_result['passed'] else "✗"
                        message = sub_result.get('message', 'Sin mensaje')
                        report_lines.append(f"   {status} {test_name}.{sub_test}: {message}")
        
        # Problemas específicos
        report_lines.append(f"\n⚠️  PROBLEMAS IDENTIFICADOS:")
        problems_found = False
        
        for test_name, result in resultados['tests'].items():
            if isinstance(result, dict) and not result.get('passed', True):
                problems_found = True
                if 'invalid_ids' in result and result['invalid_ids']:
                    report_lines.append(f"   {test_name}: IDs inválidos: {result['invalid_ids'][:5]}{'...' if len(result['invalid_ids']) > 5 else ''}")
                if 'invalid_values' in result and result['invalid_values']:
                    report_lines.append(f"   {test_name}: Valores inválidos: {result['invalid_values'][:3]}{'...' if len(result['invalid_values']) > 3 else ''}")
        
        if not problems_found:
            report_lines.append("   No se encontraron problemas críticos")
        
        report_lines.append("\n" + "=" * 60)
        
        return "\n".join(report_lines)
