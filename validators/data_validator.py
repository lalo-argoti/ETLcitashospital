"""
Validador de calidad de datos con reglas configurables
"""
import pandas as pd
import numpy as np
import re
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Callable

class DataValidator:
    """Validador de datos con métricas de calidad"""
    
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.validation_results = {}
        self.errors = []
    
    def validate_not_null(self, column: str, threshold: float = 0.95) -> Dict:
        """
        Valida que menos del threshold% de valores sean nulos
        
        Args:
            column: Nombre de la columna
            threshold: Porcentaje mínimo de valores no nulos (0-1)
        
        Returns:
            Dict con resultados de validación
        """
        total = len(self.df)
        null_count = self.df[column].isnull().sum()
        null_percentage = null_count / total if total > 0 else 0
        
        result = {
            'column': column,
            'rule': 'not_null',
            'threshold': threshold,
            'null_count': int(null_count),
            'null_percentage': round(null_percentage, 4),
            'total_rows': total,
            'passed': null_percentage <= (1 - threshold),
            'message': f"{null_count}/{total} valores nulos ({null_percentage:.2%})"
        }
        
        self.validation_results[f"{column}_not_null"] = result
        if not result['passed']:
            self.errors.append(f"Columna '{column}' tiene demasiados nulos: {null_percentage:.2%}")
        
        return result
    
    def validate_unique(self, column: str) -> Dict:
        """Valida que los valores en la columna sean únicos"""
        total = len(self.df)
        unique_count = self.df[column].nunique()
        duplicate_count = total - unique_count
        
        result = {
            'column': column,
            'rule': 'unique',
            'total_rows': total,
            'unique_count': unique_count,
            'duplicate_count': duplicate_count,
            'passed': duplicate_count == 0,
            'message': f"{duplicate_count} duplicados encontrados"
        }
        
        self.validation_results[f"{column}_unique"] = result
        if not result['passed']:
            self.errors.append(f"Columna '{column}' tiene {duplicate_count} valores duplicados")
        
        return result
    
    def validate_format(self, column: str, pattern: str, description: str = "") -> Dict:
        """
        Valida formato usando regex
        
        Args:
            column: Nombre de la columna
            pattern: Patrón regex
            description: Descripción del formato esperado
        """
        if column not in self.df.columns:
            return {
                'column': column,
                'rule': 'format',
                'passed': False,
                'message': f"Columna '{column}' no existe"
            }
        
        # Filtrar valores no nulos
        non_null = self.df[column].dropna()
        total_non_null = len(non_null)
        
        if total_non_null == 0:
            result = {
                'column': column,
                'rule': 'format',
                'pattern': pattern,
                'total_checked': 0,
                'invalid_count': 0,
                'passed': True,
                'message': "Sin valores para validar"
            }
        else:
            # Aplicar regex
            invalid = non_null[~non_null.astype(str).str.match(pattern)]
            invalid_count = len(invalid)
            
            result = {
                'column': column,
                'rule': 'format',
                'pattern': pattern,
                'description': description,
                'total_checked': total_non_null,
                'invalid_count': invalid_count,
                'invalid_percentage': round(invalid_count / total_non_null, 4),
                'passed': invalid_count == 0,
                'message': f"{invalid_count}/{total_non_null} valores con formato incorrecto"
            }
            
            if invalid_count > 0:
                self.errors.append(f"Columna '{column}': {invalid_count} valores no cumplen formato '{description}'")
                # Guardar ejemplos de valores inválidos
                result['invalid_examples'] = invalid.head(5).tolist()
        
        self.validation_results[f"{column}_format"] = result
        return result
    
    def validate_range(self, column: str, min_val: Optional[float] = None, 
                      max_val: Optional[float] = None) -> Dict:
        """Valida que los valores estén dentro de un rango"""
        if column not in self.df.columns:
            return {
                'column': column,
                'rule': 'range',
                'passed': False,
                'message': f"Columna '{column}' no existe"
            }
        
        non_null = self.df[column].dropna()
        total_non_null = len(non_null)
        
        if total_non_null == 0:
            result = {
                'column': column,
                'rule': 'range',
                'min': min_val,
                'max': max_val,
                'total_checked': 0,
                'out_of_range_count': 0,
                'passed': True,
                'message': "Sin valores para validar"
            }
        else:
            mask = pd.Series(True, index=non_null.index)
            
            if min_val is not None:
                mask &= (non_null >= min_val)
            
            if max_val is not None:
                mask &= (non_null <= max_val)
            
            out_of_range = non_null[~mask]
            out_of_range_count = len(out_of_range)
            
            result = {
                'column': column,
                'rule': 'range',
                'min': min_val,
                'max': max_val,
                'total_checked': total_non_null,
                'out_of_range_count': out_of_range_count,
                'out_of_range_percentage': round(out_of_range_count / total_non_null, 4),
                'passed': out_of_range_count == 0,
                'message': f"{out_of_range_count}/{total_non_null} valores fuera de rango"
            }
            
            if out_of_range_count > 0:
                self.errors.append(f"Columna '{column}': {out_of_range_count} valores fuera de rango [{min_val}, {max_val}]")
                result['out_of_range_examples'] = out_of_range.head(5).tolist()
        
        self.validation_results[f"{column}_range"] = result
        return result
    
    def validate_value_set(self, column: str, allowed_values: List) -> Dict:
        """Valida que los valores estén en un conjunto permitido"""
        if column not in self.df.columns:
            return {
                'column': column,
                'rule': 'value_set',
                'passed': False,
                'message': f"Columna '{column}' no existe"
            }
        
        non_null = self.df[column].dropna()
        total_non_null = len(non_null)
        
        if total_non_null == 0:
            result = {
                'column': column,
                'rule': 'value_set',
                'allowed_values': allowed_values,
                'total_checked': 0,
                'invalid_count': 0,
                'passed': True,
                'message': "Sin valores para validar"
            }
        else:
            invalid = non_null[~non_null.isin(allowed_values)]
            invalid_count = len(invalid)
            
            result = {
                'column': column,
                'rule': 'value_set',
                'allowed_values': allowed_values,
                'total_checked': total_non_null,
                'invalid_count': invalid_count,
                'invalid_percentage': round(invalid_count / total_non_null, 4),
                'passed': invalid_count == 0,
                'message': f"{invalid_count}/{total_non_null} valores no permitidos"
            }
            
            if invalid_count > 0:
                self.errors.append(f"Columna '{column}': {invalid_count} valores no permitidos")
                result['invalid_examples'] = invalid.head(5).tolist()
                result['unique_invalid_values'] = invalid.unique().tolist()
        
        self.validation_results[f"{column}_value_set"] = result
        return result
    
    def validate_consistency(self, column1: str, column2: str, 
                           rule_func: Callable, description: str = "") -> Dict:
        """
        Valida consistencia entre dos columnas
        
        Args:
            column1: Primera columna
            column2: Segunda columna
            rule_func: Función que recibe dos valores y retorna bool
            description: Descripción de la regla
        """
        if column1 not in self.df.columns or column2 not in self.df.columns:
            return {
                'rule': 'consistency',
                'columns': [column1, column2],
                'passed': False,
                'message': f"Columnas no existen: {column1}, {column2}"
            }
        
        # Filtrar filas donde ambas columnas no son nulas
        mask = self.df[column1].notna() & self.df[column2].notna()
        rows_to_check = self.df[mask]
        total_to_check = len(rows_to_check)
        
        if total_to_check == 0:
            result = {
                'rule': 'consistency',
                'columns': [column1, column2],
                'description': description,
                'total_checked': 0,
                'inconsistent_count': 0,
                'passed': True,
                'message': "Sin filas para validar consistencia"
            }
        else:
            # Aplicar regla de consistencia
            inconsistent_mask = rows_to_check.apply(
                lambda row: not rule_func(row[column1], row[column2]), 
                axis=1
            )
            inconsistent_count = inconsistent_mask.sum()
            
            result = {
                'rule': 'consistency',
                'columns': [column1, column2],
                'description': description,
                'total_checked': total_to_check,
                'inconsistent_count': int(inconsistent_count),
                'inconsistent_percentage': round(inconsistent_count / total_to_check, 4),
                'passed': inconsistent_count == 0,
                'message': f"{inconsistent_count}/{total_to_check} filas inconsistentes"
            }
            
            if inconsistent_count > 0:
                self.errors.append(f"Consistencia {column1}-{column2}: {inconsistent_count} inconsistencias")
                # Guardar ejemplos de inconsistencias
                examples = rows_to_check[inconsistent_mask].head(3)
                result['inconsistent_examples'] = examples[[column1, column2]].to_dict('records')
        
        self.validation_results[f"consistency_{column1}_{column2}"] = result
        return result
    
    def get_summary(self) -> Dict:
        """Retorna un resumen de todas las validaciones"""
        total_tests = len(self.validation_results)
        passed_tests = sum(1 for r in self.validation_results.values() if r['passed'])
        
        return {
            'total_tests': total_tests,
            'passed_tests': passed_tests,
            'failed_tests': total_tests - passed_tests,
            'success_rate': round(passed_tests / total_tests * 100, 2) if total_tests > 0 else 0,
            'errors': self.errors,
            'details': self.validation_results
        }
    
    def print_report(self):
        """Imprime un reporte de validación"""
        summary = self.get_summary()
        
        print("\n" + "="*80)
        print("REPORTE DE VALIDACIÓN DE DATOS")
        print("="*80)
        print(f"Total pruebas: {summary['total_tests']}")
        print(f"Pruebas exitosas: {summary['passed_tests']}")
        print(f"Pruebas fallidas: {summary['failed_tests']}")
        print(f"Tasa de éxito: {summary['success_rate']}%")
        
        if summary['errors']:
            print("\n❌ ERRORES ENCONTRADOS:")
            for error in summary['errors']:
                print(f"  - {error}")
        
        print("\n📊 DETALLE DE VALIDACIONES:")
        for test_name, result in self.validation_results.items():
            status = "✅" if result['passed'] else "❌"
            print(f"  {status} {test_name}: {result['message']}")
