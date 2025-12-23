#!/usr/bin/env python3
"""
MAIN.PY - ORQUESTADOR DE LA PRUEBA TÉCNICA
Ejecuta todo el pipeline de análisis, limpieza y generación de informes
"""

import os
import sys
import subprocess
import time
from datetime import datetime
import json

class PipelineOrchestrator:
    """Orquesta la ejecución completa de todos los scripts"""
    
    def __init__(self):
        self.start_time = time.time()
        self.results = {}
        self.script_order = [
            ("01_exploratorio.py", "Análisis Exploratorio de Datos"),
            ("02_limpieza.py", "Limpieza y Transformación de Datos"),
            ("03_validaciones.py", "Validaciones Cruzadas"),
            ("04_informe.py", "Generación de Informe Técnico")
        ]
        
    def print_header(self):
        """Imprime encabezado del pipeline"""
        print("="*80)
        print("PRUEBA TÉCNICA - INGENIERO DE DATOS - PIPELINE COMPLETO")
        print("="*80)
        print(f"Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Directorio: {os.getcwd()}")
        print("="*80)
        
    def check_requirements(self):
        """Verifica que todos los archivos necesarios existan"""
        print("\n" + "="*80)
        print("VERIFICANDO REQUISITOS")
        print("="*80)
        
        required_files = [
            'dataset_hospital.json',
            'scripts/01_exploratorio.py',
            'scripts/02_limpieza.py',
            'scripts/03_validaciones.py',
            'scripts/04_informe.py'
        ]
        
        all_ok = True
        for file in required_files:
            if os.path.exists(file):
                print(f"✅ {file}")
            else:
                print(f"❌ {file} - NO ENCONTRADO")
                all_ok = False
        
        if not all_ok:
            print("\n⚠️  Faltan archivos necesarios. Saliendo...")
            sys.exit(1)
            
        print("\n✅ Todos los requisitos verificados")
        return True
    
    def execute_script(self, script_name, description):
        """Ejecuta un script individual"""
        print(f"\n{'='*80}")
        print(f"EJECUTANDO: {description}")
        print(f"{'='*80}")
        
        script_path = f"scripts/{script_name}"
        
        if not os.path.exists(script_path):
            print(f"❌ Script no encontrado: {script_path}")
            return False
        
        try:
            # Ejecutar el script
            print(f"📁 Script: {script_name}")
            print(f"📝 Descripción: {description}")
            print(f"⏰ Inicio: {datetime.now().strftime('%H:%M:%S')}")
            print("-"*40)
            
            # Ejecutar con el intérprete de Python del entorno virtual
            result = subprocess.run(
                [sys.executable, script_path],
                capture_output=True,
                text=True,
                encoding='utf-8'
            )
            
            # Guardar resultados
            self.results[script_name] = {
                'return_code': result.returncode,
                'stdout': result.stdout[-1000:],  # Últimos 1000 caracteres
                'stderr': result.stderr,
                'timestamp': datetime.now().isoformat()
            }
            
            if result.returncode == 0:
                print(f"✅ {description} - COMPLETADO EXITOSAMENTE")
                print(f"📊 Salida (resumen):")
                # Mostrar líneas importantes de la salida
                lines = result.stdout.strip().split('\n')
                for line in lines[-10:]:  # Últimas 10 líneas
                    if line.strip():
                        print(f"   {line}")
                return True
            else:
                print(f"❌ {description} - FALLÓ")
                print(f"🔴 Error: {result.stderr[:500]}")
                return False
                
        except Exception as e:
            print(f"❌ Error ejecutando {script_name}: {e}")
            self.results[script_name] = {
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
            return False
    
    def generate_summary_report(self):
        """Genera un reporte resumen de la ejecución"""
        print("\n" + "="*80)
        print("REPORTE DE EJECUCIÓN DEL PIPELINE")
        print("="*80)
        
        elapsed_time = time.time() - self.start_time
        
        report = {
            'pipeline_execution': {
                'start_time': datetime.fromtimestamp(self.start_time).isoformat(),
                'end_time': datetime.now().isoformat(),
                'elapsed_seconds': round(elapsed_time, 2),
                'total_scripts': len(self.script_order),
                'successful_scripts': sum(1 for script, _ in self.script_order 
                                        if self.results.get(script, {}).get('return_code') == 0)
            },
            'script_results': self.results,
            'generated_files': self.list_generated_files()
        }
        
        # Guardar reporte en JSON
        with open('reportes/pipeline_execution_report.json', 'w') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # Imprimir resumen
        print(f"\n📈 RESUMEN DE EJECUCIÓN:")
        print(f"   • Tiempo total: {elapsed_time:.2f} segundos")
        print(f"   • Scripts ejecutados: {len(self.script_order)}")
        print(f"   • Scripts exitosos: {report['pipeline_execution']['successful_scripts']}")
        
        print(f"\n📁 ARCHIVOS GENERADOS:")
        for file_type, files in report['generated_files'].items():
            print(f"   • {file_type}: {len(files)} archivos")
            for file in files[:3]:  # Mostrar primeros 3 de cada tipo
                print(f"     - {file}")
            if len(files) > 3:
                print(f"     ... y {len(files)-3} más")
        
        print(f"\n📄 Reporte detallado: reportes/pipeline_execution_report.json")
        
        return report
    
    def list_generated_files(self):
        """Lista los archivos generados por el pipeline"""
        generated = {
            'datos_limpios': [],
            'reportes': [],
            'otros': []
        }
        
        # Verificar directorio datoscln/
        if os.path.exists('datoscln'):
            for file in os.listdir('datoscln'):
                if file.endswith(('.csv', '.parquet')):
                    generated['datos_limpios'].append(f"datoscln/{file}")
        
        # Verificar directorio reportes/
        if os.path.exists('reportes'):
            for file in os.listdir('reportes'):
                generated['reportes'].append(f"reportes/{file}")
        
        # Otros archivos generados
        for file in ['informe_tecnico_calidad_datos.txt', 'metricas_completas.json']:
            if os.path.exists(file):
                generated['otros'].append(file)
        
        return generated
    
    def show_final_instructions(self):
        """Muestra instrucciones finales"""
        print("\n" + "="*80)
        print("🎉 PIPELINE COMPLETADO")
        print("="*80)
        
        print(f"\n📦 PARA ENTREGAR LA PRUEBA TÉCNICA:")
        print(f"\n1. Comprimir los archivos:")
        print(f"   zip -r prueba_tecnica_carlos.zip \\")
        print(f"       reportes/ \\")
        print(f"       datoscln/ \\")
        print(f"       scripts/ \\")
        print(f"       main.py \\")
        print(f"       README.md")
        
        print(f"\n2. Verificar que el informe esté completo:")
        print(f"   cat reportes/informe_tecnico_calidad_datos.txt | head -20")
        
        print(f"\n3. (Opcional) Convertir informe a PDF:")
        print(f"   # Método 1: Copiar a Word/Google Docs")
        print(f"   # Método 2: pandoc reportes/informe_tecnico_calidad_datos.txt -o INFORME.pdf")
        
        print(f"\n📊 RESULTADOS PRINCIPALES:")
        print(f"   • Datos limpios en: datoscln/")
        print(f"   • Informe técnico en: reportes/informe_tecnico_calidad_datos.txt")
        print(f"   • Reporte de ejecución en: reportes/pipeline_execution_report.json")
    
    def run(self):
        """Ejecuta el pipeline completo"""
        self.print_header()
        
        # Verificar requisitos
        if not self.check_requirements():
            return False
        
        # Ejecutar cada script en orden
        all_success = True
        for script_name, description in self.script_order:
            success = self.execute_script(script_name, description)
            if not success:
                print(f"⚠️  El script {script_name} falló. Continuando con los siguientes...")
                # Decidir si continuar o detenerse
                if script_name == "02_limpieza.py":
                    print("❌ Script crítico falló. Deteniendo pipeline...")
                    all_success = False
                    break
            time.sleep(1)  # Pequeña pausa entre scripts
        
        # Generar reporte final
        if all_success:
            self.generate_summary_report()
            self.show_final_instructions()
        else:
            print("\n" + "="*80)
            print("❌ PIPELINE INCOMPLETO")
            print("="*80)
            print("\nAlgunos scripts fallaron. Revisa los errores arriba.")
        
        return all_success

# Versión alternativa más simple (sin subprocess)
def run_simple_pipeline():
    """Versión alternativa que importa y ejecuta los scripts directamente"""
    print("="*80)
    print("PIPELINE SIMPLIFICADO - EJECUCIÓN DIRECTA")
    print("="*80)
    
    import importlib.util
    import sys
    
    # Añadir scripts al path
    sys.path.insert(0, 'scripts')
    
    scripts = [
        ('01_exploratorio', 'Análisis Exploratorio'),
        ('02_limpieza', 'Limpieza de Datos'),
        ('03_validaciones', 'Validaciones Cruzadas'),
        ('04_informe', 'Generación de Informe')
    ]
    
    for script_module, description in scripts:
        print(f"\n▶️  Ejecutando: {description}...")
        try:
            # Importar y ejecutar el módulo
            spec = importlib.util.spec_from_file_location(
                script_module, 
                f"scripts/{script_module}.py"
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            print(f"✅ {description} completado")
        except Exception as e:
            print(f"❌ Error en {description}: {e}")
    
    print("\n" + "="*80)
    print("✅ PIPELINE COMPLETADO")
    print("="*80)

def main():
    """Función principal"""
    print("Selecciona el modo de ejecución:")
    print("1. Pipeline completo con subprocess (recomendado)")
    print("2. Pipeline simplificado (ejecución directa)")
    print("3. Solo verificar estructura")
    
    choice = input("\nOpción [1]: ").strip() or "1"
    
    if choice == "1":
        orchestrator = PipelineOrchestrator()
        success = orchestrator.run()
        sys.exit(0 if success else 1)
    elif choice == "2":
        run_simple_pipeline()
    elif choice == "3":
        # Solo verificar estructura
        orchestrator = PipelineOrchestrator()
        orchestrator.print_header()
        orchestrator.check_requirements()
        print("\n✅ Estructura verificada correctamente")
    else:
        print("❌ Opción no válida")

if __name__ == "__main__":
    main()
