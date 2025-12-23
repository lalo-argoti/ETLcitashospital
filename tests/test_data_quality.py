"""
Pruebas de calidad de datos para el dataset hospitalario
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from validators.data_validator import DataValidator
from validators.schema_validator import HospitalSchemaValidator

class TestDataQuality:
    """Pruebas de calidad de datos"""
    
    def test_pacientes_not_null_constraints(self, sample_pacientes_df):
        """Prueba que columnas críticas no tengan muchos valores nulos"""
        validator = DataValidator(sample_pacientes_df)
        
        # Columnas que no deberían tener nulos (o muy pocos)
        validator.validate_not_null('id_paciente', threshold=1.0)
        validator.validate_not_null('nombre', threshold=1.0)
        validator.validate_not_null('fecha_nacimiento', threshold=1.0)
        
        summary = validator.get_summary()
        assert summary['passed_tests'] >= 2  # Al menos 2 de 3 deberían pasar
        
    def test_pacientes_id_unique(self, sample_pacientes_df):
        """Prueba que los IDs de pacientes sean únicos"""
        validator = DataValidator(sample_pacientes_df)
        result = validator.validate_unique('id_paciente')
        
        assert result['passed'] == True
        assert result['duplicate_count'] == 0
    
    def test_sexo_normalizado(self, sample_pacientes_df):
        """Prueba que el género esté normalizado a 'M' o 'F'"""
        validator = DataValidator(sample_pacientes_df)
        result = validator.validate_value_set('sexo', ['M', 'F'])
        
        # Este test debería fallar porque hay un valor 'Male' en los datos de prueba
        assert result['passed'] == False
        assert result['invalid_count'] > 0
        assert 'Male' in result.get('unique_invalid_values', [])
    
    def test_email_format(self, sample_pacientes_df):
        """Prueba que los emails tengan formato válido"""
        validator = DataValidator(sample_pacientes_df)
        
        # Patrón regex para emails válidos
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        result = validator.validate_format('email', email_pattern, "formato de email")
        
        # Debería fallar porque hay un email inválido en los datos de prueba
        assert result['passed'] == False
        assert result['invalid_count'] > 0
    
    def test_telefono_format(self, sample_pacientes_df):
        """Prueba que los teléfonos solo contengan dígitos"""
        validator = DataValidator(sample_pacientes_df)
        
        # Solo dígitos, entre 7 y 15 caracteres
        phone_pattern = r'^\d{7,15}$'
        result = validator.validate_format('telefono', phone_pattern, "solo dígitos")
        
        assert result['passed'] == False  # Hay un teléfono 'abc123' en los datos
    
    def test_edad_rango_valido(self, sample_pacientes_df):
        """Prueba que las edades estén en un rango razonable"""
        validator = DataValidator(sample_pacientes_df)
        result = validator.validate_range('edad', min_val=0, max_val=120)
        
        # Las edades válidas deberían estar en rango
        # Nota: El valor nulo no se considera en esta validación
        assert result['passed'] == True
    
    def test_consistencia_edad_fecha_nacimiento(self, sample_pacientes_df):
        """Prueba consistencia entre edad y fecha de nacimiento"""
        validator = DataValidator(sample_pacientes_df)
        
        def edad_consistente(edad, fecha_nac):
            """Verifica que la edad sea consistente con la fecha de nacimiento"""
            if pd.isna(edad) or pd.isna(fecha_nac):
                return True  # Si alguno es nulo, no podemos validar
            
            try:
                fecha_nac_dt = pd.to_datetime(fecha_nac)
                hoy = datetime.now()
                edad_calculada = hoy.year - fecha_nac_dt.year
                
                # Ajustar si aún no ha cumplido años este año
                if (hoy.month, hoy.day) < (fecha_nac_dt.month, fecha_nac_dt.day):
                    edad_calculada -= 1
                
                # Permitir diferencia de ±1 año por posibles desfases
                return abs(edad - edad_calculada) <= 1
            except:
                return False
        
        result = validator.validate_consistency(
            'edad', 'fecha_nacimiento', 
            edad_consistente,
            "edad consistente con fecha de nacimiento"
        )
        
        assert result['passed'] == True  # Los datos de prueba son consistentes
    
    def test_citas_schema_validation(self, sample_citas_df):
        """Prueba validación de esquema para citas"""
        schema_validator = HospitalSchemaValidator()
        results = schema_validator.validate_citas_schema(sample_citas_df)
        
        # Debería pasar la validación de esquema básico
        assert results['passed'] == True
        assert len(results['missing_columns']) == 0
    
    def test_citas_costo_positivo(self, sample_citas_df):
        """Prueba que los costos sean positivos"""
        validator = DataValidator(sample_citas_df)
        result = validator.validate_range('costo', min_val=0)
        
        # Debería fallar porque hay un costo negativo en los datos de prueba
        assert result['passed'] == False
        assert result['out_of_range_count'] > 0
    
    def test_fecha_cita_valida(self, sample_citas_df):
        """Prueba que las fechas de cita tengan formato válido"""
        validator = DataValidator(sample_citas_df)
        
        # Patrón para fecha YYYY-MM-DD
        date_pattern = r'^\d{4}-\d{2}-\d{2}$'
        result = validator.validate_format('fecha_cita', date_pattern, "formato YYYY-MM-DD")
        
        # Debería fallar porque hay una fecha inválida '2025-31-12'
        assert result['passed'] == False
        assert result['invalid_count'] > 0
    
    @pytest.mark.integration
    def test_datos_reales_calidad(self, datos_reales_limpios):
        """Prueba de integración con datos reales limpios"""
        pacientes = datos_reales_limpios['pacientes']
        citas = datos_reales_limpios['citas']
        
        # Validar pacientes
        pacientes_validator = DataValidator(pacientes)
        
        # Reglas críticas para pacientes limpios
        pacientes_validator.validate_not_null('id_paciente', threshold=1.0)
        pacientes_validator.validate_not_null('nombre', threshold=1.0)
        pacientes_validator.validate_unique('id_paciente')
        pacientes_validator.validate_value_set('sexo', ['M', 'F'])
        
        pacientes_summary = pacientes_validator.get_summary()
        
        # Después de la limpieza, estas validaciones deberían pasar
        assert pacientes_summary['success_rate'] > 80, \
            f"Baja tasa de éxito en validación de pacientes: {pacientes_summary['success_rate']}%"
        
        # Validar citas
        citas_validator = DataValidator(citas)
        
        # Reglas críticas para citas limpias
        citas_validator.validate_not_null('id_cita', threshold=1.0)
        citas_validator.validate_not_null('id_paciente', threshold=1.0)
        citas_validator.validate_unique('id_cita')
        citas_validator.validate_range('costo', min_val=0)
        
        citas_summary = citas_validator.get_summary()
        
        # Reportar resultados
        print(f"\nValidación pacientes: {pacientes_summary['success_rate']}% éxito")
        print(f"Validación citas: {citas_summary['success_rate']}% éxito")
        
        # Después de limpieza, esperamos alta tasa de éxito
        assert citas_summary['success_rate'] > 70, \
            f"Baja tasa de éxito en validación de citas: {citas_summary['success_rate']}%"
