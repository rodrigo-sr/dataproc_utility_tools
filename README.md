# Dataproc Utility Tools - Validação de Schema e Geração de Dados Sintéticos

Este projeto contém ferramentas para análise de qualidade de dados, validação de schemas CSV e geração de dados sintéticos com inconsistências controladas.

## Componentes do Projeto
- `schema_analyzer.py`: Classe `SchemaAnalyzer` para analisar o schema de arquivos CSV e identificar inconsistências.
- `csv_data_generator.py`: Classe `CSVDataGenerator` para gerar datasets sintéticos com inconsistências controladas.
- `clean_dataset.py`: Classe `CleanDatasetProcessor` para limpar datasets com base em análise de schema.
- `utils.py`: Funções utilitárias para manipulação de schemas e tipos de dados.
- `validate_schema.py`: Script de linha de comando para validar um ou mais CSVs usando `SchemaAnalyzer` e salvar relatórios JSON.

## Requisitos
- Python 3.9+
- Dependências:
  - pandas
  - numpy
  - faker

Instalação rápida das dependências:
```
pip install pandas numpy faker
```

## Instalação

### Instalação Local (Desenvolvimento)
1. Clone ou copie os arquivos do projeto
2. Instale as dependências:
   ```
   pip install pandas numpy faker
   ```
3. Ou instale diretamente com o pyproject.toml:
   ```
   pip install -e .
   ```

### Instalação via pip (após build)
Após build do pacote:
```
pip install dist/dq-tools-*.whl
```

## Estrutura do Projeto
- `schema_analyzer.py` - Análise de schema e detecção de inconsistências
- `csv_data_generator.py` - Geração de datasets sintéticos
- `clean_dataset.py` - Processamento e limpeza de datasets
- `utils.py` - Funções auxiliares
- `validate_schema.py` - Interface de linha de comando
- `test_datasets/` - Exemplos de CSVs de teste (opcional)
- `cleaned/` - Dados limpos gerados pelo processador
- `test_final/` - Exemplos de arquivos de análise

## Uso

### 1. Análise de Schema (Validação de Dados)

#### Linha de Comando
Analisar um ou mais CSVs e gerar relatórios JSON:

```
dq-validate test_datasets/dataset_03_mixed_type_inconsistencies.csv
```

Validar múltiplos arquivos e salvar os JSONs em um diretório específico:

```
dq-validate test_datasets/dataset_00_numeric_inconsistencies.csv \
            test_datasets/dataset_01_date_inconsistencies.csv \
            --output-dir analyses
```

#### Parâmetros Opcionais
- `--output-dir`: Diretório de saída dos relatórios JSON (padrão: `.`)
- `--chunksize`: Tamanho do lote de leitura (padrão: 10000)
- `--sample-size`: Amostras por coluna (padrão: 5)
- `--inconsistency-sample-size`: Amostras de inconsistências (padrão: 3)
- `--numeric-threshold`: Limiar para coluna numérica (padrão: 0.95)
- `--mixed-threshold`: Limiar para coluna mista (padrão: 0.7)
- `--high-null-threshold`: Alerta de nulos (padrão: 0.1)

Exemplo com parâmetros customizados:

```
dq-validate test_datasets/dataset_02_text_inconsistencies.csv \
  --output-dir analyses --chunksize 5000 --sample-size 8 \
  --inconsistency-sample-size 5 --numeric-threshold 0.9 \
  --mixed-threshold 0.6 --high-null-threshold 0.2
```

#### Utilização Programática
```python
from schema_analyzer import SchemaAnalyzer

# Criar uma instância do analisador com parâmetros personalizados
analyzer = SchemaAnalyzer(
    chunksize=5000,
    sample_size=8,
    numeric_threshold=0.9,
    mixed_threshold=0.6
)

# Analisar um arquivo CSV
schema = analyzer.analyze('data/my_file.csv')

# Imprimir um resumo da análise
analyzer.print_summary()

# Salvar o schema em formato JSON
analyzer.save_schema('output/schema_analysis.json')
```

### 2. Geração de Dados Sintéticos

#### Gerar Datasets de Teste
```python
from csv_data_generator import CSVDataGenerator

# Criar um gerador com seed fixa para reprodutibilidade
gen = CSVDataGenerator(seed=123)

# Gerar múltiplos datasets com diferentes tipos de inconsistências
gen.generate_test_datasets(n_datasets=5, base_rows=5000, output_dir='test_datasets')
```

#### Gerar Dataset Específico com Inconsistências
```python
from csv_data_generator import CSVDataGenerator

gen = CSVDataGenerator(seed=123)

# Gerar um dataset com tipos específicos de inconsistências
gen.generate_specific_inconsistency(
    'test_datasets/specific_inconsistencies.csv', 
    ['numeric', 'date', 'text', 'null'], 
    n_rows=1000
)
```

#### Utilização via Linha de Comando
```
python -c "from csv_data_generator import CSVDataGenerator; gen=CSVDataGenerator(seed=123); gen.generate_test_datasets(n_datasets=5, base_rows=5000, output_dir='test_datasets')"
```

```
python -c "from csv_data_generator import CSVDataGenerator; gen=CSVDataGenerator(seed=123); gen.generate_specific_inconsistency('test_datasets/specific_inconsistencies.csv', ['numeric','date','text','null'], n_rows=1000)"
```

### 3. Limpeza de Dados

#### Linha de Comando
Separar registros consistentes e inconsistentes com base em um arquivo JSON de análise:

```
python clean_dataset.py test_final/schema_analysis_application_train.json --csv application_train.csv --output-dir cleaned
```

#### Parâmetros
- `analysis_json`: Caminho para o JSON gerado por schema_analyzer.save_schema
- `--csv`: Caminho para o CSV a ser tratado (padrão: usa summary.file_path do JSON)
- `--output-dir`: Diretório de saída (padrão: cleaned)
- `--consistent-name`: Nome do CSV consistente (padrão: <file>_consistent.csv)
- `--inconsistent-name`: Nome do CSV inconsistente (padrão: <file>_inconsistent.csv)

#### Utilização Programática
```python
from clean_dataset import CleanDatasetProcessor

processor = CleanDatasetProcessor()

# Processar diretamente um DataFrame com um schema
df_consistent, df_inconsistent = processor.process_dataframe(df, schema)

# Ou processar a partir de um arquivo de análise
consistent_path, inconsistent_path, n_consistent, n_inconsistent = processor.process_from_analysis(
    analysis_json='test_final/schema_analysis_application_train.json',
    csv_path='application_train.csv',
    output_dir='cleaned'
)
```

### 4. Integração com Pandas

#### Usando Schema para Carregar Dados com Tipos Corretos
```python
from utils import schema_from_analysis_json
import pandas as pd

# Carregar o schema de um arquivo de análise
dtypes = schema_from_analysis_json('test_final/schema_analysis_application_train.json')

# Usar os tipos inferidos ao carregar o CSV
df = pd.read_csv('application_train.csv', dtype=dtypes)
```

## Formato de Saída

### Arquivo JSON de Análise
O arquivo de saída contém:
- `schema`: Detalhes de cada coluna (tipo inferido, amostras, inconsistências)
- `summary`: Informações gerais sobre o dataset
- `analysis_parameters`: Parâmetros usados na análise

Exemplo de estrutura:
```json
{
  "schema": {
    "nome_da_coluna": {
      "inferred_type": "numeric|categorical|boolean|mixed",
      "samples": ["exemplo1", "exemplo2", ...],
      "null_count": 5,
      "null_percentage": 0.01,
      "inconsistencies": [...],
      "numeric_ratio": 0.95,
      "total_values_analyzed": 1000
    }
  },
  "summary": {
    "file_path": "caminho/do/arquivo.csv",
    "file_name": "arquivo.csv",
    "total_columns": 10,
    "total_rows": 10000,
    "columns_with_issues": 2,
    "total_issues": 5,
    "type_distribution": {"numeric": 3, "categorical": 6, "boolean": 1},
    "analysis_timestamp": "2025-10-19T10:30:00.123456"
  },
  "analysis_parameters": {...}
}
```

## Tipos de Inconsistências Detectadas

- `non_numeric_in_numeric_column`: Valores não numéricos em colunas numericamente dominantes
- `numeric_in_categorical_column`: Valores numéricos em colunas categoricamente dominantes
- `high_null_percentage`: Porcentagem excessiva de valores nulos
- Tipos mistos: Presença de diferentes tipos de dados na mesma coluna

## Dicas e Limitações

- **Arquivos grandes**: Ajuste `--chunksize` para equilibrar memória e performance
- **Amostras**: `--sample-size` e `--inconsistency-sample-size` controlam exemplos exibidos/armazenados
- **Tipos booleanos**: Detecção é heurística baseada em palavras-chave comuns (ex: 'true', 'false', 'sim', 'não')
- **Codificação**: Use CSVs em `utf-8` para melhores resultados
- **Performance**: Para datasets muito grandes, reduza os parâmetros de amostragem
- **Tipos de dados**: O sistema tenta inferir tipos automaticamente mas pode ser necessário ajuste manual

## Desenvolvimento

- **Testes manuais**: Use `test_datasets/` ou gere seus próprios dados com `csv_data_generator.py`
- **Scripts auxiliares**: Use `clean_dataset.py` para limpeza e `utils.py` para funções auxiliares
- **Análise de desempenho**: Use diferentes valores de chunksize para otimizar a leitura de datasets grandes

## Casos de Uso

### 1. Validação de Pipeline de Dados
```python
# Validar dados recebidos antes de processamento
analyzer = SchemaAnalyzer()
schema = analyzer.analyze('incoming_data.csv')
analyzer.print_summary()
# Verificar se há inconsistências críticas antes de prosseguir
```

### 2. Geração de Dados de Teste
```python
# Gerar dados com inconsistências conhecidas para testar pipelines
gen = CSVDataGenerator()
gen.generate_specific_inconsistency('test_data_with_issues.csv', ['numeric'], n_rows=1000)
```

### 3. Padronização de Dados
```python
# Converter datasets para tipos consistentes com base na análise
dtypes = schema_from_analysis_json('analysis.json')
df = pd.read_csv('data.csv', dtype=dtypes)
```

### 4. Monitoramento de Qualidade
```python
# Comparar schemas de diferentes versões de datasets
# Identificar degradação de qualidade ao longo do tempo
```