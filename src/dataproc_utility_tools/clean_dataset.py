import argparse
import json
import os
from typing import Dict, Any, Tuple

import numpy as np
import pandas as pd


class CleanDatasetProcessor:
    NULLISH_TOKENS = {"", " ", "nan", "null", "none", "na", "undefined", "n/a", "N/A", "NULL", "NaN", "None"}
    BOOL_TRUE = {"true", "1", "yes", "y", "sim", "s", "verdadeiro"}
    BOOL_FALSE = {"false", "0", "no", "n", "nao", "não", "falso"}

    @staticmethod
    def _is_nullish(value: Any) -> bool:
        if pd.isna(value):
            return True
        try:
            s = str(value).strip()
        except Exception:
            return False
        return s in CleanDatasetProcessor.NULLISH_TOKENS

    @staticmethod
    def _is_numeric(value: Any) -> bool:
        if CleanDatasetProcessor._is_nullish(value):
            return False
        if isinstance(value, (int, float)):
            return True
        try:
            s = str(value).strip()
            if s.lower() in {"true", "false", "yes", "no"}:
                return False
            float(s)
            return True
        except (ValueError, TypeError):
            return False

    @staticmethod
    def _to_boolean(value: Any) -> Tuple[bool, Any]:
        if CleanDatasetProcessor._is_nullish(value):
            return True, np.nan
        if isinstance(value, (bool, np.bool_)):
            return True, bool(value)
        s = str(value).strip().lower()
        if s in CleanDatasetProcessor.BOOL_TRUE:
            return True, True
        if s in CleanDatasetProcessor.BOOL_FALSE:
            return True, False
        return False, value

    @staticmethod
    def _decide_main_type(col_schema: Dict[str, Any]) -> str:
        inferred = col_schema.get("inferred_type", "categorical")
        if inferred == "mixed":
            ratio = float(col_schema.get("numeric_ratio", 0.0))
            return "numeric" if ratio >= 0.5 else "categorical"
        return inferred

    def process_dataframe(self, df: pd.DataFrame, schema: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        inconsistent_mask = pd.Series(False, index=df.index)
        converted = {}

        for col in df.columns:
            col_info = schema.get(col, {})
            main_type = self._decide_main_type(col_info)
            series = df[col]

            if main_type == "numeric":
                cleaned = series.where(~series.astype(str).str.strip().isin(self.NULLISH_TOKENS), other=np.nan)
                numeric = pd.to_numeric(cleaned, errors="coerce")
                invalid = ~series.isna() & numeric.isna() & ~series.astype(str).str.strip().isin(self.NULLISH_TOKENS)
                inconsistent_mask |= invalid
                converted[col] = numeric

            elif main_type == "boolean":
                valid_flags = []
                out_vals = []
                for v in series:
                    ok, out = self._to_boolean(v)
                    valid_flags.append(ok)
                    out_vals.append(out)
                converted_series = pd.Series(out_vals, index=series.index)
                invalid = ~pd.Series(valid_flags, index=series.index)
                inconsistent_mask |= invalid
                converted[col] = converted_series

            elif main_type == "categorical":
                looks_numeric = series.apply(self._is_numeric)
                invalid = looks_numeric & ~series.isna()
                inconsistent_mask |= invalid
                converted[col] = series.astype(str).where(~series.isna(), other=np.nan)

            else:
                converted[col] = series

        df_converted = pd.DataFrame(converted)
        df_inconsistent = df_converted[inconsistent_mask].copy()
        df_consistent = df_converted[~inconsistent_mask].copy()
        return df_consistent, df_inconsistent

    def process_from_analysis(self, analysis_json: str, csv_path: str = None, output_dir: str = "cleaned",
                              consistent_name: str = None, inconsistent_name: str = None) -> Tuple[str, str, int, int]:
        with open(analysis_json, "r", encoding="utf-8") as f:
            analysis = json.load(f)

        schema = analysis.get("schema", {})
        summary = analysis.get("summary", {})
        csv_path = csv_path or summary.get("file_path")
        if not csv_path:
            raise ValueError("Caminho do CSV não encontrado. Informe csv_path ou garanta que summary.file_path exista no JSON.")
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV não encontrado: {csv_path}")

        base_name = os.path.splitext(os.path.basename(csv_path))[0]
        consistent_name = consistent_name or f"{base_name}_consistent.csv"
        inconsistent_name = inconsistent_name or f"{base_name}_inconsistent.csv"

        os.makedirs(output_dir, exist_ok=True)

        df = pd.read_csv(csv_path)
        df_consistent, df_inconsistent = self.process_dataframe(df, schema)

        consistent_out = os.path.join(output_dir, consistent_name)
        inconsistent_out = os.path.join(output_dir, inconsistent_name)

        df_consistent.to_csv(consistent_out, index=False, encoding="utf-8")
        df_inconsistent.to_csv(inconsistent_out, index=False, encoding="utf-8")

        return consistent_out, inconsistent_out, len(df_consistent), len(df_inconsistent)


def main():
    parser = argparse.ArgumentParser(
        description="Separa registros inconsistentes e aplica conversões de tipo com base no JSON de análise de schema."
    )
    parser.add_argument("analysis_json", help="Caminho para o JSON gerado por schema_analyzer.save_schema")
    parser.add_argument("--csv", help="Caminho para o CSV a ser tratado (padrão: usa summary.file_path do JSON)")
    parser.add_argument("--output-dir", default="cleaned", help="Diretório de saída (padrão: cleaned)")
    parser.add_argument("--consistent-name", default=None, help="Nome do CSV consistente (padrão: <file>_consistent.csv)")
    parser.add_argument("--inconsistent-name", default=None, help="Nome do CSV inconsistente (padrão: <file>_inconsistent.csv)")

    args = parser.parse_args()

    processor = CleanDatasetProcessor()
    consistent_out, inconsistent_out, n_consistent, n_inconsistent = processor.process_from_analysis(
        analysis_json=args.analysis_json,
        csv_path=args.csv,
        output_dir=args.output_dir,
        consistent_name=args.consistent_name,
        inconsistent_name=args.inconsistent_name,
    )

    print(f"Registros consistentes: {n_consistent:,}")
    print(f"Registros inconsistentes: {n_inconsistent:,}")
    print(f"Arquivo consistente salvo em: {consistent_out}")
    print(f"Arquivo inconsistente salvo em: {inconsistent_out}")


if __name__ == "__main__":
    main()
