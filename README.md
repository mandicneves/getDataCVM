# Dados CVM

Este pacote permite obter e processar os demonstrativos contábeis de empresas com capital aberto no Brasil diretamente da Comissão de Valores Mobiliários (CVM). Ele facilita a extração, organização e análise dos dados financeiros das companhias listadas.

Dados disponíveis em: https://dados.cvm.gov.br/

## Funcionalidades

- **Download automático** dos demonstrativos contábeis diretamente da CVM.
- **Processamento e limpeza** dos dados brutos para facilitar a análise.
- **Conversão dos dados** em formatos estruturados como DataFrames do pandas.
- **Suporte a múltiplos períodos** e tipos de demonstrativos (Balanço Patrimonial, DRE, DFC, etc.).

## Instalação

Para instalar o pacote, utilize:

```bash
pip install getDataCVM
```

## Exemplo de Uso

```python
from getDataCVM import DFP

# Criar uma instância do coletor de dados
dfp = DFP()

# Baixar dados do balanço patrimonial ativo das empresas entre 2010 e 2020
df = dfp.get_data("bpa_ind", 2010, 2020)

# Exibir os primeiros registros
df.head()
```

## Contribuindo

Sinta-se à vontade para contribuir com melhorias. Faça um fork do repositório, implemente as mudanças e envie um pull request!

## Licença

Este projeto é distribuído sob a licença MIT.

