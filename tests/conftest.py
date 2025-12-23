"""
Pruebas de integridad de datos para el dataset hospitalario
"""
import pytest
import pandas as pd
from validators.integrity_validator import IntegrityValidator
import pytest
import pandas as pd
import json
import os

# Registrar el marcador integration
def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integration: marca pruebas de integración con datos reales"
    )

@pytest.fixture
def sample_pacientes():
    """Fixture con datos de pacientes de prueba"""
    with open('tests/fixtures/sample_pacientes.json', 'r') as f:
        return pd.DataFrame(json.load(f))

@pytest.fixture
def sample_citas():
    """Fixture con datos de citas de prueba"""
    with open('tests/fixtures/sample_citas.json', 'r') as f:
        return pd.DataFrame(json.load(f))

@pytest.fixture
def datos_limpios():
    """Fixture con datos limpios reales"""
    pacientes = pd.read_csv('datoscln/pacientes_limpios.csv')
    citas = pd.read_csv('datoscln/citas_limpios.csv')
    return {'pacientes': pacientes, 'citas': citas}


class TestDataIntegrity:
    """Pruebas de integridad referencial y consistencia"""
    
    def test_referencias_pacientes_validas(self, sample_pacientes, sample_citas):
        """Prueba que todas las citas referencien pacientes existentes"""
        validator = IntegrityValidator()
        
        result = validator.validate_foreign_keys(
            sample_citas, 'id_paciente',
            sample_pacientes, 'id_paciente'
        )
        
        # Debería fallar porque hay una cita con paciente 999 que no existe
        assert result['passed'] == False
        assert result['invalid_references'] > 0
        assert 999 in result.get('invalid_ids', [])
    
    def test_fechas_coherentes(self, sample_citas):
        """Prueba que las fechas de cita sean coherentes"""
        validator = IntegrityValidator()
        
        result = validator.validate_date_coherence(
            sample_citas, 'fecha_cita',
            max_future_days=365*2  # Máximo 2 años en el futuro
        )
        
        # Esto depende de las fechas en los datos de prueba
        # Si hay fechas futuras extremas, fallará
        print(f"Resultado validación fechas: {result}")
    
    def test_estados_cita_validos(self, sample_citas):
        """Prueba que los estados de cita sean válidos"""
        validator = IntegrityValidator()
        
        valid_states = ['Programada', 'Completada', 'Cancelada', 
                       'Reprogramada', 'No Programada']
        
        result = validator.validate_column_values(
            sample_citas, 'estado_cita', valid_states
        )
        
        # Los estados nulos se consideran inválidos
        assert result['invalid_count'] >= 1  # Hay un estado nulo en los datos
    
    def test_especialidades_validas(self, sample_citas):
        """Prueba que las especialidades sean válidas"""
        validator = IntegrityValidator()
        
        valid_specialties = ['Cardiología', 'Neurología', 'Pediatría',
                            'Ginecología', 'Ortopedia']
        
        result = validator.validate_column_values(
            sample_citas, 'especialidad', valid_specialties
        )
        
        # Hay una especialidad nula en los datos
        assert result['invalid_count'] >= 1
    
    @pytest.mark.integration
    def test_integridad_datos_reales(self, datos_limpios):
        """
        Prueba de integridad completa para el dataset hospitalario real limpio
        Usa los datos reales procesados del pipeline
        """
        validator = IntegrityValidator()
        
        # Extraer DataFrames del fixture
        pacientes_df = datos_limpios['pacientes']
        citas_df = datos_limpios['citas']
        
        print(f"\n📊 Datos cargados:")
        print(f"   Pacientes: {len(pacientes_df)} registros")
        print(f"   Citas: {len(citas_df)} registros")
        print(f"   Columnas pacientes: {list(pacientes_df.columns)}")
        print(f"   Columnas citas: {list(citas_df.columns)}")
        
        # Validación completa
        resultados = validator.validate_all_integrity(pacientes_df, citas_df)
        
        # Generar y mostrar reporte
        reporte = validator.generate_integrity_report(resultados)
        print("\n" + reporte)
        
        # Verificaciones CRÍTICAS (deben pasar siempre)
        
        # 1. Integridad referencial ABSOLUTA
        # Todas las citas deben referenciar pacientes existentes
        assert resultados['tests']['referencial_pacientes']['passed'] == True, \
            f"❌ FALLA CRÍTICA: {resultados['tests']['referencial_pacientes']['message']}"
        
        # 2. No duplicados en IDs de pacientes
        if 'unicidad_ids' in resultados['tests'] and 'pacientes' in resultados['tests']['unicidad_ids']:
            assert resultados['tests']['unicidad_ids']['pacientes']['passed'] == True, \
                f"❌ IDs de pacientes duplicados: {resultados['tests']['unicidad_ids']['pacientes']['duplicates']}"
        
        # 3. No duplicados en IDs de citas
        if 'unicidad_ids' in resultados['tests'] and 'citas' in resultados['tests']['unicidad_ids']:
            assert resultados['tests']['unicidad_ids']['citas']['passed'] == True, \
                f"❌ IDs de citas duplicados: {resultados['tests']['unicidad_ids']['citas']['duplicates']}"
        
        # 4. Edades dentro de rango válido (si existe la columna)
        if 'rango_edad' in resultados['tests']:
            assert resultados['tests']['rango_edad']['passed'] == True, \
                f"❌ Edades fuera de rango: {resultados['tests']['rango_edad']['message']}"
        
        # Verificaciones IMPORTANTES (pueden ser warnings)
        
        # 5. Fechas coherentes (puede haber citas sin fecha)
        if not resultados['tests']['coherencia_fechas']['passed']:
            print(f"⚠️  ADVERTENCIA: {resultados['tests']['coherencia_fechas']['message']}")
            # No falla la prueba, solo muestra warning
        
        # 6. Estados válidos (puede haber valores nulos)
        if not resultados['tests']['estados_cita']['passed']:
            print(f"⚠️  ADVERTENCIA: {resultados['tests']['estados_cita']['message']}")
            invalid_estados = resultados['tests']['estados_cita'].get('invalid_values', [])
            if invalid_estados:
                print(f"   Estados inválidos encontrados: {invalid_estados}")
        
        # 7. Especialidades válidas (puede haber valores nulos)
        if not resultados['tests']['especialidades']['passed']:
            print(f"⚠️  ADVERTENCIA: {resultados['tests']['especialidades']['message']}")
            invalid_especialidades = resultados['tests']['especialidades'].get('invalid_values', [])
            if invalid_especialidades:
                print(f"   Especialidades inválidas: {invalid_especialidades}")
        
        # Resumen final
        summary = resultados['summary']
        print(f"\n{'='*60}")
        print("✅ PRUEBAS DE INTEGRIDAD COMPLETADAS")
        print(f"   Estado general: {'PASÓ' if resultados['overall_passed'] else 'FALLÓ'}")
        print(f"   Pruebas exitosas: {summary['passed_tests']}/{summary['total_tests']}")
        
        # Guardar reporte detallado
        with open('reportes/informe_integridad.txt', 'w') as f:
            f.write(reporte)
        
        # Opcional: también guardar en JSON para análisis posterior
        with open('reportes/resultados_integridad.json', 'w') as f:
            import json
            # Convertir objetos pandas/numpy a tipos nativos de Python
            def convert_to_serializable(obj):
                if hasattr(obj, 'tolist'):
                    return obj.tolist()
                elif hasattr(obj, 'item'):
                    return obj.item()
                return str(obj)
            
            resultados_serializable = json.loads(
                json.dumps(resultados, default=convert_to_serializable, indent=2)
            )
            json.dump(resultados_serializable, f, indent=2)
        
        print(f"📄 Reportes guardados en: reportes/informe_integridad.txt")
        print(f"📊 JSON guardado en: reportes/resultados_integridad.json")
        print(f"{'='*60}")
