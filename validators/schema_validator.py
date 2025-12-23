"""
Validador de esquema de datos para el dataset hospitalario
"""
import pandas as pd
from typing import Dict, List

class HospitalSchemaValidator:
    """Valida la estructura y tipos de datos del dataset hospitalario"""
    
    # Esquema esperado para pacientes
    PACIENTES_SCHEMA = {
        'id_paciente': {'type': 'int64', 'nullable': False, 'unique': True},
        'nombre': {'type': 'object', 'nullable': False},
        'fecha_nacimiento': {'type': 'object', 'nullable': False},
        'edad': {'type': 'float64', 'nullable': True, 'min': 0, 'max': 120},
        'sexo': {'type': 'object', 'nullable': True, 'allowed_values': ['M', 'F']},
        'email': {'type': 'object', 'nullable': True},
        'telefono': {'type': 'object', 'nullable': True},
        'ciudad': {'type': 'object', 'nullable': True}
    }
    
    # Esquema esperado para citas médicas
    CITAS_SCHEMA = {
        'id_cita': {'type': 'object', 'nullable': False, 'unique': True},
        'id_paciente': {'type': 'int64', 'nullable': False},
        'fecha_cita': {'type': 'object', 'nullable': True},
        'especialidad': {'type': 'object', 'nullable': True, 
                        'allowed_values': ['Cardiología', 'Neurología', 'Pediatría', 
                                          'Ginecología', 'Ortopedia']},
        'medico': {'type': 'object', 'nullable': True},
        'costo': {'type': 'float64', 'nullable': True, 'min': 0, 'max': 1000},
        'estado_cita': {'type': 'object', 'nullable': True,
                       'allowed_values': ['Programada', 'Completada', 'Cancelada', 
                                         'Reprogramada', 'No Programada']}
    }
    
    def __init__(self):
        self.errors = []
        self.warnings = []
    
    def validate_pacientes_schema(self, df: pd.DataFrame) -> Dict:
        """Valida el esquema de la tabla de pacientes"""
        return self._validate_schema(df, self.PACIENTES_SCHEMA, 'pacientes')
    
    def validate_citas_schema(self, df: pd.DataFrame) -> Dict:
        """Valida el esquema de la tabla de citas"""
        return self._validate_schema(df, self.CITAS_SCHEMA, 'citas')
    
    def _validate_schema(self, df: pd.DataFrame, schema: Dict, table_name: str) -> Dict:
        """Valida un DataFrame contra un esquema"""
        results = {
            'table': table_name,
            'total_columns_expected': len(schema),
            'total_columns_found': len(df.columns),
            'missing_columns': [],
            'extra_columns': [],
            'type_mismatches': [],
            'constraint_violations': [],
            'passed': True
        }
        
        # 1. Verificar columnas existentes
        expected_columns = set(schema.keys())
        actual_columns = set(df.columns)
        
        missing_columns = expected_columns - actual_columns
        extra_columns = actual_columns - expected_columns
        
        if missing_columns:
            results['missing_columns'] = list(missing_columns)
            results['passed'] = False
            self.errors.append(f"Tabla {table_name}: Columnas faltantes: {missing_columns}")
        
        if extra_columns:
            results['extra_columns'] = list(extra_columns)
            self.warnings.append(f"Tabla {table_name}: Columnas extras: {extra_columns}")
        
        # 2. Verificar tipos de datos para columnas existentes
        for column in expected_columns.intersection(actual_columns):
            column_spec = schema[column]
            actual_dtype = str(df[column].dtype)
            expected_dtype = column_spec['type']
            
            # Verificar tipo (con cierta flexibilidad)
            type_ok = self._check_type_compatibility(actual_dtype, expected_dtype)
            
            if not type_ok:
                results['type_mismatches'].append({
                    'column': column,
                    'expected': expected_dtype,
                    'actual': actual_dtype
                })
                results['passed'] = False
                self.errors.append(
                    f"Tabla {table_name}.{column}: Tipo esperado {expected_dtype}, "
                    f"encontrado {actual_dtype}"
                )
        
        return results
    
    def _check_type_compatibility(self, actual: str, expected: str) -> bool:
        """Verifica compatibilidad de tipos con cierta flexibilidad"""
        type_map = {
            'int64': ['int64', 'int32', 'float64', 'float32'],
            'float64': ['float64', 'float32', 'int64', 'int32'],
            'object': ['object']
        }
        
        return expected in type_map and actual in type_map[expected]
    
    def validate_data_integrity(self, pacientes_df: pd.DataFrame, 
                               citas_df: pd.DataFrame) -> Dict:
        """Valida integridad referencial entre tablas"""
        results = {
            'foreign_key_violations': 0,
            'date_consistency_issues': 0,
            'passed': True
        }
        
        # 1. Validar que todas las citas tienen pacientes válidos
        pacientes_ids = set(pacientes_df['id_paciente'].unique())
        citas_pacientes = set(citas_df['id_paciente'].unique())
        
        invalid_references = citas_pacientes - pacientes_ids
        
        if invalid_references:
            results['foreign_key_violations'] = len(invalid_references)
            results['passed'] = False
            self.errors.append(
                f"Integridad referencial: {len(invalid_references)} citas "
                f"referencian pacientes no existentes"
            )
            results['invalid_patient_ids'] = list(invalid_references)[:10]  # Primeros 10
        
        # 2. Validar consistencia de fechas (opcional)
        try:
            # Intentar convertir fechas para validación
            citas_df['fecha_cita_dt'] = pd.to_datetime(citas_df['fecha_cita'], errors='coerce')
            pacientes_df['fecha_nacimiento_dt'] = pd.to_datetime(
                pacientes_df['fecha_nacimiento'], errors='coerce'
            )
            
            # Verificar que fechas de cita no sean anteriores a fecha de nacimiento
            # (Esto sería más complejo, se puede implementar si es necesario)
            
        except Exception as e:
            self.warnings.append(f"No se pudo validar consistencia de fechas: {e}")
        
        return results
    
    def get_validation_report(self) -> str:
        """Genera un reporte de validación"""
        report = []
        
        if self.errors:
            report.append("❌ ERRORES DE VALIDACIÓN:")
            for error in self.errors:
                report.append(f"  - {error}")
        
        if self.warnings:
            report.append("⚠️  ADVERTENCIAS:")
            for warning in self.warnings:
                report.append(f"  - {warning}")
        
        if not self.errors and not self.warnings:
            report.append("✅ Todas las validaciones pasaron exitosamente")
        
        return "\n".join(report)
