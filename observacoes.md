# Regras de normalização

As colunas originais devem ser preservadas. As funções criam novas colunas
normalizadas para comparação e auditoria. CEP, código do município e demais
identificadores continuam como texto, pois não representam quantidades e podem
conter zero à esquerda.

## Mapeamento entre as bases

| Campo normalizado | Base de busca | CNEFE | Observação |
|---|---|---|---|
| `logradouro_normalizado` | `consulta_logradouro` | `NOM_TIPO_SEGLOGR` + `NOM_TITULO_SEGLOGR` + `NOM_SEGLOGR` | Texto sem acentos, pontuação e espaços repetidos |
| `numero_normalizado` | `consulta_numero` | `NUM_ENDERECO` + `DSC_MODIFICADOR` | Preserva números alfanuméricos e converte indicadores de sem número para `sn` |
| `bairro_normalizado` | `consulta_bairro` | — | Usado somente na base de busca |
| `localidade_normalizada` | — | `DSC_LOCALIDADE` | Campo auxiliar; não é assumido automaticamente como bairro |
| `cep_normalizado` | `consulta_cep` | `CEP` | Deve ter exatamente 8 dígitos |
| `cep_valido` | derivado | derivado | Indica se o CEP original contém exatamente 8 dígitos |
| `municipio_normalizado` | `consulta_municipio` | `COD_MUNICIPIO` | Busca: 6 dígitos; CNEFE: 7 dígitos, removendo o dígito verificador final |
| `municipio_valido` | derivado | derivado | Validação feita de acordo com a base de origem |
| `complemento_normalizado` | `consulta_complemento` | campos de complemento do CNEFE | Normalização conservadora, sem tentar interpretar o conteúdo |

## Decisões e limitações

- Valores nulos continuam nulos; não são convertidos para a string `"null"`.
- `NUM_ENDERECO = 0` não é convertido automaticamente para `sn`. A conversão só
  ocorre quando o número ou o modificador informa explicitamente `S/N`, `SN`,
  `SEM NUMERO` ou `SEM NRO`.
- Números alfanuméricos são preservados: `102 E` vira `102e`.
- CEP e município nunca são convertidos para inteiro, evitando a perda de zeros
  à esquerda.
- CEP inválido não recebe zeros artificiais. Ele vira nulo na coluna
  `cep_normalizado`, enquanto `cep_valido` fica falso.
- `COD_MUNICIPIO` e `COD_SETOR` originais não devem ser sobrescritos.
- Bairro e localidade são campos auxiliares e podem receber peso menor no
  relacionamento. Uma localidade do CNEFE não é necessariamente um bairro.
- Todas as linhas da base de busca são mantidas, inclusive se a quantidade
  encontrada for diferente da estimativa descrita no enunciado.

## Casos de verificação

| Entrada | Saída esperada |
|---|---|
| `Av. Antônio Carlos` | `avenida antonio carlos` |
| `JD. PRIMAVERA` | `jardim primavera` |
| `BAIRRO` | nulo |
| `S/N` | `sn` |
| `102 E` | `102e` |
| `48.030-260` | `48030260` |
| `01234-567` | `01234567` (zero inicial preservado) |
| CEP com 7 ou 9 dígitos | nulo e `cep_valido = false` |
| município da busca `290070` | `290070` |
| município do CNEFE `2900702` | `290070` |
| município do CNEFE `0123456` | `012345` (zero inicial preservado) |
| município com quantidade incorreta de dígitos | nulo e `municipio_valido = false` |
