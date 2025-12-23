# Prueba Técnica - Ingeniero de Datos
## Sistema Integrado de Limpieza y Validación de Datos Hospitalarios

### 🚀 EJECUCIÓN RÁPIDA

```bash
# 1. Asegúrate de tener Python 3.8+ y los requisitos
pip install pandas numpy matplotlib seaborn

# 2. Ejecuta el pipeline principal
python 02_limpieza_integrado.py
```
# 3. Revisa los resultados en las carpetas generadas

```
├── dataset_hospital.json
├── datoscln                           #Datos filtrados
│   ├── citas_limpios.csv
│   ├── citas_limpios.parquet
│   ├── pacientes_limpios.csv
│   └── pacientes_limpios.parquet
├── main.py
├── __pycache__
│   ├── cargar_datos.cpython-38.pyc
│   └── s0201_limpieza_fechas.cpython-38.pyc
├── README.md
├── reportes
│   ├── informe_tecnico_calidad_datos.txt
│   └── pipeline_execution_report.json
├── requirements_testing.txt
├── scripts
│   ├── 01_exploratorio.py
│   ├── 02_limpieza.py
│   ├── 03_validaciones.py
│   ├── 04_informe.py
│   ├── cargar_datos.py
│   └── __pycache__
│       └── cargar_datos.cpython-38.pyc
├── tests
│   ├── conftest.py
│   ├── fixtures
│   │   ├── sample_citas.json
│   │   └── sample_pacientes.json
│   ├── __init__.py
│   ├── test_data_quality.py
│   ├── test_data_validation.py
│   └── test_integrity.py
├── validators
│   ├── data_validator.py
│   ├── __init__.py
│   ├── integrity_validator.py
│   └── schema_validator.py
├── expectations
│   ├── checkpoints
│   ├── expectation_suites
│   └── great_expectations.yml

```

🔧 CARACTERÍSTICAS PRINCIPALES
Corrección inteligente de fechas: Detecta y corrige formatos incorrectos

Detección de ambigüedades: Identifica fechas con múltiples interpretaciones

Validaciones cruzadas: Verifica integridad entre tablas

Reportes organizados: Todo en carpeta reportes/

Visualizaciones: Gráficos para análisis visual

Métricas detalladas: JSON con todas las métricas

📊 MÉTRICAS GENERADAS
El sistema calcula y reporta:

Antes/después de cada transformación

Correcciones aplicadas a fechas

Fechas ambiguas para revisión manual

Validaciones de integridad referencial

Score de calidad por paciente

🎯 PARA LA PRUEBA TÉCNICA
Esto demuestra:

Pensamiento analítico para problemas complejos

Diseño de algoritmos heurísticos

Organización profesional de outputs

Documentación clara y completa

Capacidad de integración modular

# ⚠️ NOTAS IMPORTANTES
El módulo 02_01_correccion_fechas.py puede usarse independientemente

Las fechas ambiguas se marcan en columna fecha_ambigua

Todos los reportes van a reportes/ para fácil organización

Se generan logs detallados en reportes/logs/

# 📞 SOPORTE
Para problemas o preguntas:

Verifica que dataset_hospital.json esté en el directorio

Revisa los logs en reportes/logs/

Ejecuta módulos por separado para debugging


## 🎯 **BENEFICIOS DE ESTA ARQUITECTURA:**

1. **Modular**: Cada componente es independiente y reusable
2. **Organizado**: Todos los outputs en carpetas específicas  
3. **Profesional**: Listo para entregar en prueba técnica
4. **Escalable**: Fácil de extender con nuevas funcionalidades
5. **Documentado**: Incluye README con instrucciones claras

**¿Listo para ejecutar este sistema integrado?** Solo necesitas tener el archivo `dataset_hospital.json` en el mismo directorio y ejecutar `02_limpieza_integrado.py`.
