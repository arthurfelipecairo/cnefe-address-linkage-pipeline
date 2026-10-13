# Guia de apresentação — CNEFE Address Linkage Pipeline

Roteiro para uma apresentação de aproximadamente 10 minutos. Os 10 slides já
incluem a capa. O conteúdo abaixo corresponde ao repositório
`cnefe-address-linkage-pipeline` e aos resultados existentes em `output/`.

## Orientação visual geral

- Formato: 16:9.
- Fundo: branco ou azul-marinho muito escuro.
- Cores de destaque: azul para PySpark, amarelo para Elasticsearch e verde paIa
  resultados aprovados.
- Fontes: Aptos, Arial ou Calibri.
- Tamanhos mínimos: 32 pt para títulos e 20 pt para o corpo.
- Use pouco texto no slide. O detalhamento fica na fala do apresentador.
- Prefira capturas do próprio projeto, diagramas simples e gráficos editáveis.
- Não use imagens decorativas que não ajudem a explicar a solução.

---

## Slide 1 — CNEFE Address Linkage Pipeline

### Objetivo do slide

Apresentar o projeto, o problema resolvido e as tecnologias principais.

### Conteúdo para colocar no slide

**CNEFE Address Linkage Pipeline**

Vinculação de endereços da Bahia a setores censitários

PySpark | Elasticsearch | Docker | GitHub

Nome do aluno  
Disciplina e professor  
Data da apresentação

### Visual recomendado

Use uma capa simples. Coloque o título no lado esquerdo e, no lado direito, um
fluxo pequeno com os textos `Endereço consultado`, `Busca no CNEFE` e
`Setor censitário`.

### O que falar

> O projeto recebe uma base pequena de endereços de consulta e procura, no
> CNEFE da Bahia, o endereço mais compatível e seu setor censitário. Eu construí
> uma pipeline reproduzível com PySpark para tratar os dados, Elasticsearch para
> pesquisar candidatos e Docker para executar o ambiente.

### Tempo sugerido

30 segundos.

---

## Slide 2 — Problema e bases de dados

### Objetivo do slide

Explicar o tamanho do problema e por que uma comparação direta de textos não é
suficiente.

### Conteúdo para colocar no slide

**Base CNEFE da Bahia**

- 9.047.296 linhas
- 34 colunas
- Endereço, CEP, município e setor censitário

**Base de busca**

- 107 endereços e 6 colunas
- 78 municípios
- 11 números, 35 bairros e 67 complementos ausentes

**Desafio**

Os mesmos endereços aparecem com abreviações, acentos, pontuação e campos
incompletos.

### Visual recomendado

Mostre duas fontes de dados com tamanhos diferentes. À esquerda, um bloco maior
representando o CNEFE. À direita, um bloco menor representando as 107 consultas.
No centro, use uma lupa com a frase `encontrar o setor correto`.

### O que falar

> A diferença de escala é grande. A referência tem mais de nove milhões de
> linhas, enquanto a entrada possui 107 consultas. Uma junção simples não
> funcionaria bem porque há abreviações, valores ausentes e diferenças de
> escrita. Por isso, o projeto primeiro padroniza os campos e depois combina
> buscas exatas e aproximadas.

### Tempo sugerido

50 segundos.

---

## Slide 3 — Arquitetura da solução

### Objetivo do slide

Mostrar o fluxo completo e a função de cada tecnologia.

### Conteúdo para colocar no slide

Use somente este fluxo principal:

`CSV CNEFE e CSV de busca` → `PySpark` → `Elasticsearch` → `Score e decisão` → `Arquivos de saída`

Abaixo do fluxo, use estas descrições curtas:

- **PySpark:** leitura, perfil, normalização e filtro dos dados.
- **Elasticsearch:** indexação e recuperação rápida de candidatos.
- **Docker Compose:** ambiente reproduzível com pipeline e Elasticsearch.
- **GitHub:** versionamento do código e documentação, sem publicar os dados.

### Visual recomendado

Crie um diagrama horizontal com cinco etapas. Use setas simples e destaque que
o Elasticsearch recebe apenas os municípios presentes na busca.

### O que falar

> O PySpark lê e normaliza as duas bases. Antes da indexação, a pipeline descobre
> os 78 municípios presentes nas consultas e filtra o CNEFE. O Elasticsearch
> armazena os endereços relevantes e retorna candidatos. Em seguida, o Python
> calcula um score próprio, toma a decisão e grava resultado, auditoria e
> métricas. O Docker garante que todos executem as mesmas versões.

### Tempo sugerido

1 minuto.

---

## Slide 4 — Perfil e preparação dos dados

### Objetivo do slide

Explicar a primeira etapa prática do desenvolvimento.

### Conteúdo para colocar no slide

**Etapa 1: conhecer os dados antes de transformá-los**

- Contagem de linhas e colunas.
- Inspeção do esquema e de amostras.
- Contagem de valores ausentes e distintos.
- Identificação de 107 consultas, embora o enunciado estimasse 100.
- Preservação de todas as linhas reais da entrada.

No rodapé:

`src/profile_data.py` e `src/spark_utils.py`

### Visual recomendado

Use uma captura do terminal com a execução do perfil da base de busca. Destaque
as 107 linhas e os valores ausentes. Evite mostrar o terminal inteiro.

### O que falar

> Eu comecei pelo perfil dos dados, sem alterar nenhuma coluna. Essa etapa
> confirmou tipos, volume e campos ausentes. Também mostrou que o arquivo real
> tinha 107 registros. A pipeline usa a quantidade real e garante que nenhuma
> consulta seja descartada durante o processamento.

### Tempo sugerido

50 segundos.

---

## Slide 5 — Normalização com PySpark

### Objetivo do slide

Mostrar como endereços escritos de formas diferentes se tornam comparáveis.

### Conteúdo para colocar no slide

**Etapa 2: criar campos normalizados sem apagar os originais**

| Entrada | Valor normalizado |
|---|---|
| `Av. Antônio Carlos` | `avenida antonio carlos` |
| `JD. PRIMAVERA` | `jardim primavera` |
| `S/N` | `sn` |
| `102 E` | `102e` |
| `48.030-260` | `48030260` |
| município `2900702` | `290070` |

**Regras importantes**

- CEP e município continuam como texto para preservar zeros à esquerda.
- CEP inválido vira nulo no campo normalizado.
- Bairro da busca e localidade do CNEFE permanecem campos distintos.

No rodapé: `src/normalizacao.py` e `observacoes.md`

### Visual recomendado

Use a tabela de antes e depois. Destaque com cor os acentos removidos, as
abreviações expandidas e o zero inicial preservado.

### O que falar

> A normalização usa funções nativas do PySpark, que podem processar milhões de
> linhas sem uma função Python linha a linha. O código cria colunas auxiliares e
> preserva os valores originais para auditoria. CEP e município são
> identificadores, então continuam como texto e não perdem zeros à esquerda.

### Tempo sugerido

1 minuto e 10 segundos.

---

## Slide 6 — Indexação no Elasticsearch

### Objetivo do slide

Explicar como o volume do CNEFE foi preparado para pesquisa eficiente.

### Conteúdo para colocar no slide

**Etapa 3: preparar e indexar os endereços relevantes**

1. Filtrar o CNEFE pelos 78 municípios consultados.
2. Manter somente campos necessários para busca e auditoria.
3. Remover duplicidades pelo identificador do endereço.
4. Reparticionar o DataFrame e enviar lotes de 1.000 documentos.
5. Indexar 3.756.323 endereços.

**Otimizações**

- `streaming_bulk` evita carregar toda a base na memória do Python.
- O índice possui campos para busca exata e aproximada.
- O intervalo de atualização fica desativado durante a carga e retorna a 1 s no final.

No rodapé: `src/index_cnefe.py` e `elasticsearch/index_cnefe.json`

### Visual recomendado

Mostre um funil: `9.047.296 linhas` → `78 municípios` →
`3.756.323 endereços únicos indexados`. Ao lado, mostre pequenos lotes entrando
no símbolo do Elasticsearch.

### O que falar

> Não era necessário indexar toda a Bahia porque todas as consultas tinham
> município. O filtro reduz tempo e espaço sem eliminar municípios válidos. A
> carga ocorre como um fluxo de lotes. Isso evita trazer milhões de registros de
> uma só vez para a memória do processo Python.

### Tempo sugerido

1 minuto.

---

## Slide 7 — Busca, score e decisão

### Objetivo do slide

Explicar como a pipeline escolhe um setor e como trata a incerteza.

### Conteúdo para colocar no slide

**Etapa 4: recuperar candidatos por estratégias complementares**

- Combinações exatas de CEP, logradouro e número.
- CEP e número com tolerância a ruído no logradouro.
- Busca aproximada dentro do mesmo município.

**Score de vinculação**

| Evidência | Peso |
|---|---:|
| Similaridade do logradouro | 45 |
| Número igual | 25 |
| CEP igual | 20 |
| Bairro ou localidade similar | 7 |
| Complemento similar | 3 |

**Regra de decisão**

- `ENCONTRADO`: score mínimo 70 e margem mínima de 2 pontos.
- `AMBIGUO`: há candidato forte, mas a diferença entre setores é pequena.
- `NAO_ENCONTRADO`: nenhum candidato adequado ou score abaixo do limite.

No rodapé: `src/linkage.py`

### Visual recomendado

Use um exemplo com dois candidatos. Mostre o primeiro com score 86 e o segundo
com 75. A margem de 11 permite classificar o primeiro como encontrado. Em um
segundo exemplo pequeno, use 86 e 85 para ilustrar um caso ambíguo.

### O que falar

> O score do Elasticsearch serve para recuperar candidatos, mas não decide o
> vínculo sozinho. A pipeline recalcula todos com a mesma regra de negócio. O
> município funciona como restrição obrigatória. Depois, logradouro, número,
> CEP, localidade e complemento formam um score de até 100 pontos. A margem
> protege contra falsos positivos quando dois setores são quase equivalentes.

### Tempo sugerido

1 minuto e 20 segundos.

---

## Slide 8 — Entregas e controle de qualidade

### Objetivo do slide

Mostrar que a solução produz resultado rastreável, não apenas um CSV final.

### Conteúdo para colocar no slide

**Etapa 5: gravar e validar as entregas**

| Entrega | Finalidade |
|---|---|
| `resultado.csv` | Consulta original, setor, status, método, score e margem |
| `auditoria.csv` e Parquet | Candidatos, ranking e evidências usadas no score |
| `metricas.json` | Cobertura, ambiguidade, métodos e distribuição dos scores |
| `validacao.json` | Preservação das 107 linhas e consistência dos status |
| `indexacao.json` | Volume e escopo da carga no Elasticsearch |
| `pipeline.log` | Histórico operacional da execução |

**Validação estrutural aprovada**

- 107 linhas de entrada e 107 linhas de saída.
- Nenhuma coluna original foi alterada.
- Casos ambíguos ou não encontrados não recebem setor automaticamente.

### Visual recomendado

Use uma captura de três linhas de `resultado.csv` abertas no VS Code ou no
Excel. Oculte colunas menos importantes e destaque status, setor e score.

### O que falar

> O resultado preserva a entrada e acrescenta as informações da vinculação. A
> auditoria registra todos os candidatos considerados e as evidências do score,
> permitindo revisar as decisões. A validação também impede que um caso
> ambíguo receba setor automaticamente.

### Tempo sugerido

1 minuto.

---

## Slide 9 — Execução reproduzível e testes

### Objetivo do slide

Explicar como outra pessoa pode executar e verificar o projeto.

### Conteúdo para colocar no slide

**Docker Compose**

```powershell
docker compose up -d elasticsearch
docker compose build pipeline
docker compose run --rm pipeline all
```

**Testes automatizados**

```powershell
docker compose run --rm --entrypoint python pipeline `
  -m unittest discover -s tests -v
```

**Resultado:** 11 testes aprovados.

Os testes cobrem normalização, CEP, município, estratégias de busca, score,
ranking e decisão.

**Publicação segura no GitHub**

`raw/`, `output/`, `.env`, chaves e certificados são ignorados pelo Git.

### Visual recomendado

Use uma captura curta do Docker Desktop com os serviços ou do terminal com os
11 testes aprovados. Ao lado, mostre a estrutura `pipeline + elasticsearch`.

### O que falar

> O Dockerfile instala Python, Java e as bibliotecas. O Compose conecta a
> pipeline ao Elasticsearch pelo nome do serviço. Assim, a execução não depende
> das versões instaladas no computador do avaliador. O repositório publica
> apenas código e documentação, enquanto os dados e segredos permanecem fora do
> GitHub.

### Tempo sugerido

1 minuto.

---

## Slide 10 — Resultados, limitações e próximos passos

### Objetivo do slide

Encerrar com os resultados reais e demonstrar entendimento das limitações.

### Conteúdo para colocar no slide

**Resultado das 107 consultas**

| Status | Quantidade | Taxa |
|---|---:|---:|
| Encontrado | 50 | 46,73% |
| Ambíguo | 42 | 39,25% |
| Não encontrado | 15 | 14,02% |

**Interpretação correta**

Essas taxas descrevem a cobertura da pipeline. Elas não representam precisão ou
recall porque não existe uma base rotulada com os setores corretos.

**Próximos passos**

- Revisar os 42 casos ambíguos com o arquivo de auditoria.
- Criar uma amostra rotulada e medir precisão e recall.
- Recalibrar pesos, score mínimo e margem com base nessa amostra.
- Em produção, habilitar autenticação e TLS no Elasticsearch.

### Visual recomendado

Use um gráfico de barras horizontais com 50, 42 e 15. Ao lado, coloque uma caixa
discreta com a limitação do padrão-ouro e os próximos passos.

### O que falar

> A pipeline encontrou um setor para 50 consultas e separou 42 casos para
> revisão, em vez de assumir uma resposta insegura. Os 15 restantes não
> atingiram o limite. Esses números medem o comportamento do sistema, mas não a
> acurácia. O próximo avanço seria revisar uma amostra, criar um padrão-ouro e
> calibrar os parâmetros com métricas objetivas.

### Tempo sugerido

1 minuto.

---

## Perguntas que o professor pode fazer

### Por que usar Spark em uma base de apenas 107 consultas?

O Spark foi usado principalmente porque a base CNEFE possui mais de nove milhões
de linhas. A mesma ferramenta normaliza as duas bases e mantém uma pipeline
coerente.

### Por que usar Elasticsearch em vez de uma junção no Spark?

O relacionamento não depende apenas de igualdade. O Elasticsearch recupera
candidatos com combinações exatas e busca aproximada. Depois, uma regra própria
calcula o score final de forma auditável.

### Por que não preencher o setor dos casos ambíguos?

Dois setores podem ter scores muito próximos. Deixar o setor vazio nesses casos
reduz o risco de entregar um falso positivo como se fosse uma decisão segura.

### O que significa a margem de score?

É a diferença entre o melhor score e o segundo melhor score de outro setor. Uma
margem pequena indica que a decisão é incerta.

### A taxa de encontrados é a precisão do sistema?

Não. Precisão exige comparar o resultado com respostas corretas conhecidas. Sem
um padrão-ouro, o projeto apresenta apenas cobertura, ambiguidade e distribuição
dos scores.

### Por que CEP e município são strings?

São identificadores, não quantidades. Mantê-los como texto preserva zeros à
esquerda e evita alterações no código original.

### O projeto está seguro para produção?

O Compose está limitado ao desenvolvimento local. Em produção, o Elasticsearch
precisa de autenticação, TLS e gerenciamento seguro das credenciais.

## Checklist antes da apresentação

- Substituir nome, disciplina, professor e data na capa.
- Executar `docker compose ps` e confirmar que o ambiente funciona.
- Tirar capturas legíveis do perfil, resultado e testes.
- Conferir se nenhuma captura exibe caminhos pessoais ou dados sensíveis.
- Ensaiar a apresentação em aproximadamente 10 minutos.
- Não afirmar que 46,73% representa acurácia.
- Levar o repositório aberto no VS Code para responder perguntas sobre o código.
