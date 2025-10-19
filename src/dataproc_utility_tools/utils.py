import json
from typing import Dict, Any


def _map_inferred_type_to_dtype(inferred_type: str) -> str:
    """
    Mapeia o tipo inferido do analisador para um dtype do pandas.

    Regras:
    - numeric -> 'float64' (cobre ints/floats e NaN)
    - boolean -> 'boolean' (pandas nullable BooleanDtype)
    - categorical/mixed/unknown -> 'string' (robusto para leitura)
    """
    t = (inferred_type or "").lower()
    if t == "numeric":
        return "float64"
    if t == "boolean":
        return "boolean"
    # 'categorical', 'mixed', 'unknown' e quaisquer outros caem como string
    return "string"


def schema_from_analysis_json(json_path: str) -> Dict[str, Any]:
    """
    Lê um arquivo JSON de análise de schema (gerado pelo SchemaAnalyzer)
    e retorna um schema fixo para uso no pandas.read_csv via parâmetro `dtype`.

    Parâmetros
    - json_path: caminho do arquivo JSON de análise (ex.: test_final/schema_analysis_*.json)

    Retorna
    - dict: mapeamento { coluna: dtype_pandas }

    Exemplo
    >>> dtypes = schema_from_analysis_json('test_final/schema_analysis_application_train.json')
    >>> import pandas as pd
    >>> df = pd.read_csv('application_train.csv', dtype=dtypes)
    """
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Arquivo JSON não encontrado: {json_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON inválido em {json_path}: {e}")

    # O arquivo pode ter a estrutura {"schema": {...}} ou já ser o mapa de colunas
    if isinstance(data, dict) and "schema" in data and isinstance(data["schema"], dict):
        columns = data["schema"]
    else:
        columns = data if isinstance(data, dict) else {}

    if not columns:
        raise ValueError("Estrutura do JSON de análise não contém informações de schema.")

    dtype_map: Dict[str, Any] = {}
    for col, info in columns.items():
        inferred_type = None
        if isinstance(info, dict):
            inferred_type = info.get("inferred_type")
        dtype_map[col] = _map_inferred_type_to_dtype(inferred_type)

    return dtype_map


def schema_to_pyspark_struct(json_path: str) -> str:
    """
    Converte um arquivo JSON de análise de schema em uma string de StructType do PySpark.
    
    Esta função lê um arquivo JSON gerado pelo SchemaAnalyzer e converte o schema
    analisado em uma string representando um StructType do PySpark que pode ser
    utilizada com eval() para criar o schema real.
    
    Parâmetros
    - json_path: caminho do arquivo JSON de análise gerado pelo SchemaAnalyzer
    
    Retorna
    - str: String representando o StructType do PySpark pronto para uso com eval()
    
    Exemplo de uso:
    >>> pyspark_schema_str = schema_to_pyspark_struct('analise_schema.json')
    >>> from pyspark.sql.types import *
    >>> schema = eval(pyspark_schema_str)
    >>> df = spark.read.csv('dados.csv', header=True, schema=schema)
    
    Formato de retorno:
    'StructType([StructField("coluna1", StringType(), True), StructField("coluna2", DoubleType(), True)])'
    """
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Arquivo JSON não encontrado: {json_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON inválido em {json_path}: {e}")

    # Extrair schema do JSON (pode estar em data["schema"] ou diretamente em data)
    if isinstance(data, dict) and "schema" in data and isinstance(data["schema"], dict):
        columns = data["schema"]
    else:
        columns = data if isinstance(data, dict) else {}

    if not columns:
        raise ValueError("Estrutura do JSON de análise não contém informações de schema.")

    # Mapear tipos inferidos para tipos do PySpark
    def _map_to_pyspark_type(inferred_type: str) -> str:
        """Mapeia tipo inferido para tipo PySpark"""
        t = (inferred_type or "").lower()
        if t == "numeric":
            return "DoubleType()"  # Sempre DoubleType para numéricos conforme solicitado
        elif t == "boolean":
            return "BooleanType()"
        else:
            # Para categorical, mixed, unknown ou qualquer outro tipo
            return "StringType()"

    # Construir lista de StructFields
    struct_fields = []
    
    for col_name, col_info in columns.items():
        # Obter tipo inferido
        inferred_type = None
        if isinstance(col_info, dict):
            inferred_type = col_info.get("inferred_type")
        
        # Determinar tipo PySpark
        pyspark_type = _map_to_pyspark_type(inferred_type)
        
        # Determinar nullability (True por padrão, mas pode ser ajustado se necessário)
        # Para simplificação, vamos manter como True para todos
        nullable = "True"
        
        # Adicionar StructField à lista
        field_str = f'StructField("{col_name}", {pyspark_type}, {nullable})'
        struct_fields.append(field_str)
    
    # Construir string final do StructType
    fields_str = ", ".join(struct_fields)
    schema_str = f'StructType([{fields_str}])'
    
    return schema_str