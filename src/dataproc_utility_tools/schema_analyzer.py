"""Módulo para análise de schema de arquivos CSV com detecção de inconsistências."""

import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd


class SchemaAnalyzer:
    """Classe para análise de schema de arquivos CSV com detecção de inconsistências.
    
    Attributes:
        chunksize (int): Tamanho do lote para leitura
        sample_size (int): Quantidade de amostras por coluna
        inconsistency_sample_size (int): Quantidade de amostras de inconsistências
        numeric_threshold (float): Threshold para classificação como numérico (0-1)
        mixed_threshold (float): Threshold para classificação como misto (0-1)
        high_null_threshold (float): Threshold para alerta de muitos nulos (0-1)

    """

    def __init__(self,
                 chunksize: int = 10000,
                 sample_size: int = 5,
                 inconsistency_sample_size: int = 3,
                 numeric_threshold: float = 0.95,
                 mixed_threshold: float = 0.7,
                 high_null_threshold: float = 0.1) -> None:

        self.chunksize = chunksize
        self.sample_size = sample_size
        self.inconsistency_sample_size = inconsistency_sample_size
        self.numeric_threshold = numeric_threshold
        self.mixed_threshold = mixed_threshold
        self.high_null_threshold = high_null_threshold

        # Resultados da análise
        self.schema = {}
        self.analysis_summary = {}
        self.file_path = None

    def analyze(self, file_path: str) -> dict[str, Any]:
        """Analisa o schema do arquivo CSV.
        
        Args:
            file_path (str): Caminho para o arquivo CSV
            
        Returns:
            Dict: Schema analisado

        """
        self.file_path = file_path

        if not Path(file_path).exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")

        column_stats = defaultdict(lambda: {
            "samples": set(),
            "numeric_count": 0,
            "non_numeric_count": 0,
            "null_count": 0,
            "inferred_type": "unknown",
            "inconsistencies": [],
            "non_numeric_samples": set(),
            "numeric_samples": set(),
            "total_count": 0,
        })

        try:
            # Ler e processar o arquivo em lotes
            for chunk_idx, chunk in enumerate(pd.read_csv(file_path, chunksize=self.chunksize, low_memory=False)):
                print(f"Processando lote {chunk_idx + 1}...")

                for column in chunk.columns:
                    stats = column_stats[column]
                    col_data = chunk[column]

                    # Coletar amostras representativas
                    self._collect_samples(stats, col_data)

                    # Analisar tipos e inconsistências
                    self._analyze_column_data(stats, col_data)

            # Processamento final após todos os lotes
            self._finalize_analysis(column_stats)

        except Exception as e:
            print(f"Erro ao processar o arquivo: {e}")
            return {}

        # Preparar resultado final
        self.schema = self._prepare_final_schema(column_stats)
        self.analysis_summary = self._generate_summary()

        return self.schema

    def _collect_samples(self, stats: dict, col_data: pd.Series) -> None:
        """Coleta amostras de valores da coluna."""
        chunk_samples = col_data.dropna().unique()
        for sample in chunk_samples[:2]:  # Poucas amostras por lote
            if len(stats["samples"]) < self.sample_size and pd.notna(sample):
                stats["samples"].add(self._convert_to_python_types(sample))

    def _analyze_column_data(self, stats: dict, col_data: pd.Series) -> None:
        """Analisa os dados de uma coluna específica."""
        for value in col_data:
            stats["total_count"] += 1

            if pd.isna(value):
                stats["null_count"] += 1
                continue

            value_converted = self._convert_to_python_types(value)
            if self._is_numeric(value_converted):
                stats["numeric_count"] += 1
                # Coletar amostras de valores numéricos para detecção em colunas categóricas
                if len(stats["numeric_samples"]) < self.inconsistency_sample_size:
                    stats["numeric_samples"].add(value_converted)
            else:
                stats["non_numeric_count"] += 1
                # Coletar amostras de valores não numéricos para detecção em colunas numéricas
                if len(stats["non_numeric_samples"]) < self.inconsistency_sample_size:
                    stats["non_numeric_samples"].add(value_converted)

    def _finalize_analysis(self, column_stats: dict) -> None:
        """Processamento final após analisar todos os lotes."""
        for col, stats in column_stats.items():
            total_non_null = stats["numeric_count"] + stats["non_numeric_count"]

            if total_non_null > 0:
                numeric_ratio = stats["numeric_count"] / total_non_null

                # Determinar tipo inferido
                stats["inferred_type"] = self._infer_column_type(numeric_ratio)

                # Detectar inconsistências
                self._detect_inconsistencies(stats, total_non_null)

                # Detectar valores nulos excessivos
                self._detect_high_nulls(stats)

            # Verificar se é coluna booleana
            self._detect_boolean_type(stats)

    def _infer_column_type(self, numeric_ratio: float) -> str:
        """Infere o tipo da coluna baseado na razão numérica."""
        if numeric_ratio > self.numeric_threshold:
            return "numeric"
        if numeric_ratio > self.mixed_threshold:
            return "mixed"
        return "categorical"

    def _detect_inconsistencies(self, stats: dict, total_non_null: int) -> None:
        """Detecta inconsistências nos dados da coluna."""
        # Inconsistências em colunas numéricas/mistas
        if stats["inferred_type"] in ["numeric", "mixed"] and stats["non_numeric_count"] > 0:
            non_numeric_samples = list(stats["non_numeric_samples"])[:self.inconsistency_sample_size]
            if non_numeric_samples:
                stats["inconsistencies"].append({
                    "type": "non_numeric_in_numeric_column",
                    "message": f'Encontrados {stats["non_numeric_count"]} valores não numéricos ({stats["non_numeric_count"]/total_non_null:.1%})',
                    "count": stats["non_numeric_count"],
                    "percentage": stats["non_numeric_count"] / total_non_null,
                    "samples": non_numeric_samples,
                })

        # Inconsistências em colunas categóricas
        if stats["inferred_type"] == "categorical" and stats["numeric_count"] > 0:
            numeric_samples = list(stats["numeric_samples"])[:self.inconsistency_sample_size]
            if numeric_samples:
                stats["inconsistencies"].append({
                    "type": "numeric_in_categorical_column",
                    "message": f'Encontrados {stats["numeric_count"]} valores numéricos ({stats["numeric_count"]/total_non_null:.1%})',
                    "count": stats["numeric_count"],
                    "percentage": stats["numeric_count"] / total_non_null,
                    "samples": numeric_samples,
                })

    def _detect_high_nulls(self, stats: dict) -> None:
        """Detecta colunas com alta porcentagem de valores nulos."""
        null_percentage = stats["null_count"] / stats["total_count"]
        if null_percentage > self.high_null_threshold:
            stats["inconsistencies"].append({
                "type": "high_null_percentage",
                "message": f"Alta porcentagem de valores nulos ({null_percentage:.1%})",
                "count": stats["null_count"],
                "percentage": null_percentage,
            })

    def _detect_boolean_type(self, stats: dict) -> None:
        """Verifica se a coluna é do tipo booleano."""
        if len(stats["samples"]) <= 3:
            bool_keywords = ["true", "false", "0", "1", "yes", "no", "verdadeiro", "falso", "sim", "não"]
            if all(any(keyword in str(s).lower() for keyword in bool_keywords) for s in stats["samples"] if s is not None):
                stats["inferred_type"] = "boolean"

    def _prepare_final_schema(self, column_stats: dict) -> dict:
        """Prepara o schema final para retorno."""
        schema = {}
        for col, stats in column_stats.items():
            schema[col] = {
                "inferred_type": stats["inferred_type"],
                "samples": list(stats["samples"])[:self.sample_size],
                "null_count": int(stats["null_count"]),
                "null_percentage": float(stats["null_count"] / max(1, stats["total_count"])),
                "inconsistencies": stats["inconsistencies"],
                "numeric_ratio": float(stats["numeric_count"] / max(1, stats["numeric_count"] + stats["non_numeric_count"])),
                "total_values_analyzed": int(stats["total_count"]),
            }
        return schema

    def _generate_summary(self) -> dict:
        """Gera um resumo da análise."""
        if not self.schema:
            return {}

        total_columns = len(self.schema)
        columns_with_issues = sum(1 for col_info in self.schema.values() if col_info["inconsistencies"])
        total_issues = sum(len(col_info["inconsistencies"]) for col_info in self.schema.values())

        type_distribution = {}
        for col_info in self.schema.values():
            col_type = col_info["inferred_type"]
            type_distribution[col_type] = type_distribution.get(col_type, 0) + 1

        return {
            "file_path": self.file_path,
            "file_name": os.path.basename(self.file_path) if self.file_path else None,
            "total_columns": total_columns,
            "total_rows": next(iter(self.schema.values()))["total_values_analyzed"] if self.schema else 0,
            "columns_with_issues": columns_with_issues,
            "total_issues": total_issues,
            "type_distribution": type_distribution,
            "analysis_timestamp": pd.Timestamp.now().isoformat(),
        }

    @staticmethod
    def _convert_to_python_types(value: Any) -> Any:
        """Converte tipos numpy/pandas para tipos nativos do Python."""
        if pd.isna(value):
            return None
        if isinstance(value, (np.integer, np.int64, np.int32)):
            return int(value)
        if isinstance(value, (np.floating, np.float64, np.float32)):
            return float(value)
        if isinstance(value, (np.bool_)):
            return bool(value)
        if isinstance(value, (pd.Timestamp)):
            return value.isoformat()
        return str(value)

    @staticmethod
    def _is_numeric(value: Any) -> bool:
        """Verifica se um valor pode ser convertido para numérico."""
        if value is None or value == "":
            return False

        # Se já for número
        if isinstance(value, (int, float)):
            return True

        # Se for string, tentar converter
        try:
            str_value = str(value).strip()
            if str_value in ["", "nan", "null", "none", "na", "none", "undefined"]:
                return False
            # Verificar se é booleano
            if str_value.lower() in ["true", "false", "yes", "no"]:
                return False
            float(str_value)
            return True
        except (ValueError, TypeError):
            return False

    def save_schema(self, output_file: str = "schema_analysis.json") -> None:
        """Salva o schema analisado em um arquivo JSON.
        
        Args:
            output_file (str): Caminho do arquivo de saída

        """
        if not self.schema:
            raise ValueError("Nenhum schema analisado. Execute analyze() primeiro.")

        def default_serializer(obj):
            if isinstance(obj, (np.integer, np.int64, np.int32)):
                return int(obj)
            if isinstance(obj, (np.floating, np.float64, np.float32)):
                return float(obj)
            if isinstance(obj, (np.bool_)):
                return bool(obj)
            if isinstance(obj, (np.ndarray)):
                return obj.tolist()
            if pd.isna(obj):
                return None
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

        output_data = {
            "schema": self.schema,
            "summary": self.analysis_summary,
            "analysis_parameters": {
                "chunksize": self.chunksize,
                "sample_size": self.sample_size,
                "inconsistency_sample_size": self.inconsistency_sample_size,
                "numeric_threshold": self.numeric_threshold,
                "mixed_threshold": self.mixed_threshold,
                "high_null_threshold": self.high_null_threshold,
            },
        }

        with Path(output_file).open("w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False, default=default_serializer)

        print(f"Schema salvo em: {output_file}")

    def print_summary(self) -> None:
        """Imprime um resumo da análise no console."""
        if not self.schema:
            print("Nenhum schema analisado.")
            return

        summary = self.analysis_summary

        print("\n" + "="*60)
        print("RESUMO DA ANÁLISE DE SCHEMA")
        print("="*60)
        print(f"Arquivo: {summary['file_path']}")
        print(f"Total de colunas: {summary['total_columns']}")
        print(f"Total de linhas: {summary['total_rows']:,}")
        print(f"Colunas com problemas: {summary['columns_with_issues']}")
        print(f"Total de inconsistências: {summary['total_issues']}")

        print("\nDistribuição de tipos:")
        for col_type, count in summary["type_distribution"].items():
            print(f"  - {col_type}: {count} coluna(s)")

        print("\nColunas com inconsistências:")
        for col_name, col_info in self.schema.items():
            if col_info["inconsistencies"]:
                print(f"  - {col_name}: {len(col_info['inconsistencies'])} inconsistência(s)")
                for inc in col_info["inconsistencies"]:
                    print(f"    * {inc['message']}")

    def get_column_report(self, column_name: str) -> Optional[dict]:
        """Retorna um relatório detalhado para uma coluna específica.
        
        Args:
            column_name (str): Nome da coluna
            
        Returns:
            Dict: Relatório da coluna ou None se não existir

        """
        return self.schema.get(column_name)

    def get_issues_by_type(self, issue_type: Optional[str] = None) -> list[dict]:
        """Retorna todas as inconsistências encontradas, opcionalmente filtradas por tipo.
        
        Args:
            issue_type (str): Tipo de inconsistência para filtrar
            
        Returns:
            List: Lista de inconsistências

        """
        all_issues = []
        for col_name, col_info in self.schema.items():
            for issue in col_info["inconsistencies"]:
                if issue_type is None or issue["type"] == issue_type:
                    issue_with_column = issue.copy()
                    issue_with_column["column"] = col_name
                    all_issues.append(issue_with_column)

        return all_issues



