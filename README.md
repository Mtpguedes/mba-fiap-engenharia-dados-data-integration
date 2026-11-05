# Pipeline CNPJ — Mercado de TI no Brasil

Pipeline em arquitetura medalhão (bronze → silver → gold) sobre os dados abertos do CNPJ da Receita Federal, para mapear oportunidades e riscos de saturação do mercado de empresas de TI nas regiões brasileiras.

---

## 1. Por que esta base

**Problema de negócio.** Quem vai abrir, expandir, investir ou dar crédito no setor de TI precisa saber: onde o mercado já está saturado, onde está crescendo, quantas empresas nascem e fecham por ano, com que idade, e como é a estrutura societária delas. Hoje essas respostas costumam vir de pesquisas pagas e pontuais. A base do CNPJ responde a todas com dado oficial e atualizado todo mês.

**Quem usaria na vida real:**

| Área | Pergunta que a base responde |
|---|---|
| Estratégia / expansão de empresas de TI | Em quais municípios abrir uma filial ou concentrar a força de vendas? |
| Fundos de investimento e aceleradoras | Quais regiões têm mais empresas novas de TI sobrevivendo? |
| Crédito e risco de contraparte | Este fornecedor ou cliente está ativo? Há quanto tempo? Tem sócio estrangeiro ou PJ? |
| Compliance / KYC | Quem são os sócios (tipo, país, representante legal)? |
| Poder público e entidades de fomento | Onde o setor é frágil (alta mortalidade) e precisa de incentivo? |

**A fonte é confiável?** Sim, com ressalvas conhecidas:
- É o **cadastro oficial e obrigatório** de toda pessoa jurídica no Brasil, publicado pela própria Receita Federal.
- CNAE, endereço e porte são **autodeclarados** pela empresa e nem sempre são atualizados (uma empresa pode ter mudado de atividade sem alterar o CNAE).
- Datas inválidas (`0`, vazias) e códigos fora do domínio existem em pequena proporção. Os quality checks medem isso a cada carga (tabela `silver.dq_resultados`).

**O volume justifica um pipeline?** Sim. São dezenas de milhões de estabelecimentos e de vínculos societários, em dezenas de arquivos, republicados todo mês. Não cabe em planilha, exige junção entre 10 arquivos e precisa ser reprocessado mensalmente de forma idêntica. É exatamente o caso de uso de um pipeline.

---

## 2. Origem dos dados

| Item | Valor |
|---|---|
| **Publicador** | Receita Federal do Brasil (Ministério da Fazenda) |
| **Página oficial** | [dados.gov.br — Cadastro Nacional da Pessoa Jurídica (CNPJ)](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj) |
| **Download** | Servidor de arquivos da Receita Federal, uma pasta por mês (`AAAA-MM`), com os arquivos `.zip` |
| **Formato** | CSV dentro de ZIP · **sem cabeçalho** · separador `;` · aspas `"` · encoding **ISO-8859-1** · datas como texto `yyyyMMdd` · valores com vírgula decimal |
| **Frequência de publicação** | Mensal |
| **Snapshot usado** | Setembro/2026 (`dt_referencia = 2026-09-30`) |
| **Dicionário oficial** | Documento de metadados/layout publicado pela Receita junto com os arquivos |
| **Licença** | Dados abertos governamentais |


---

## 3. Dicionário de dados (bronze)

Todas as colunas entram na bronze como `string`, exatamente como a Receita publica. A bronze acrescenta três colunas de controle: `dt_referencia` (mês da carga), `_arquivo_origem` (lineage) e `_dt_carga`.

A coluna **LGPD** marca o que é dado pessoal e o que o pipeline faz com ele na silver.

### 3.1 `bronze.empresas` — dados cadastrais da empresa (1 linha por CNPJ básico)

| Coluna | Descrição | LGPD |
|---|---|---|
| `cnpj_basico` | 8 primeiros caracteres do CNPJ (raiz da empresa) | |
| `razao_social` | Nome empresarial | **Pessoal em MEI/EI** (nome da pessoa + CPF). **Removida na silver** |
| `cod_natureza_juridica` | Código da natureza jurídica (ex.: 2062 = Sociedade Empresária Limitada) | |
| `cod_qualificacao_responsavel` | Qualificação da pessoa responsável pela empresa | |
| `capital_social` | Capital social, texto com vírgula decimal | |
| `porte_empresa` | 00 Não informado · 01 Micro empresa · 03 Empresa de pequeno porte · 05 Demais | |
| `ente_federativo_responsavel` | Preenchido só para órgãos da administração pública | |

### 3.2 `bronze.estabelecimentos` — matriz e filiais (1 linha por CNPJ completo)

| Coluna | Descrição | LGPD |
|---|---|---|
| `cnpj_basico` | Raiz do CNPJ (8) | |
| `cnpj_ordem` | Número do estabelecimento (posições 9–12) | |
| `cnpj_dv` | Dígitos verificadores (posições 13–14) | |
| `identificador_matriz_filial` | 1 Matriz · 2 Filial | |
| `nome_fantasia` | Nome fantasia | Pode ser nome de pessoa. **Removido na silver** |
| `situacao_cadastral` | 01 Nula · 02 Ativa · 03 Suspensa · 04 Inapta · 08 Baixada | |
| `data_situacao_cadastral` | Data do evento da situação atual (para baixadas, a data da baixa) | |
| `cod_motivo_situacao_cadastral` | Motivo da situação (ver `motivos`) | |
| `nome_cidade_exterior` | Cidade, para estabelecimentos no exterior | |
| `cod_pais` | País, **só para estabelecimentos no exterior** | |
| `data_inicio_atividade` | Data de início da atividade | |
| `cnae_fiscal_principal` | CNAE da atividade principal (7 dígitos) | |
| `cnae_fiscal_secundaria` | Lista de CNAEs secundários separados por vírgula | |
| `tipo_logradouro`, `logradouro`, `numero`, `complemento` | Endereço | **Pessoal em MEI** (endereço residencial). **Removidos na silver** |
| `bairro` | Bairro | Mantido (agregado o bastante para análise geográfica) |
| `cep` | CEP | **Silver mantém só os 5 primeiros dígitos** |
| `uf` | Unidade da federação (`EX` = exterior) | |
| `cod_municipio` | Município, **código da Receita (não é o código IBGE)** | |
| `ddd_1`, `telefone_1`, `ddd_2`, `telefone_2`, `ddd_fax`, `fax` | Telefones | **Pessoal. Removidos na silver** |
| `correio_eletronico` | E-mail | **Pessoal. Removido na silver** |
| `situacao_especial`, `data_situacao_especial` | Situação especial (ex.: recuperação judicial) | |

### 3.3 `bronze.socios` — quadro societário (1 linha por vínculo sócio × empresa)

| Coluna | Descrição | LGPD |
|---|---|---|
| `cnpj_basico` | Raiz do CNPJ da empresa | |
| `identificador_socio` | 1 Pessoa jurídica · 2 Pessoa física · 3 Estrangeiro | |
| `nome_socio` | Nome do sócio (ou razão social, se PJ) | **Pessoal (PF). Vira hash na silver** |
| `cnpj_cpf_socio` | CNPJ do sócio PJ, ou CPF **já mascarado pela Receita** (`***123456**`) | **Pessoal (PF). Vira hash na silver**. CNPJ de sócio PJ é mantido |
| `cod_qualificacao_socio` | Qualificação (ver `qualificacoes_socios`) | |
| `data_entrada_sociedade` | Data de entrada na sociedade | |
| `cod_pais` | País do sócio estrangeiro | |
| `representante_legal` | CPF mascarado do representante legal (`***000000**` = sem representante) | **Removido na silver** (vira flag) |
| `nome_representante` | Nome do representante legal | **Removido na silver** |
| `cod_qualificacao_representante` | Qualificação do representante | |
| `faixa_etaria` | 0 Não se aplica · 1 0–12 · 2 13–20 · 3 21–30 · 4 31–40 · 5 41–50 · 6 51–60 · 7 61–70 · 8 71–80 · 9 80+ | |

### 3.4 `bronze.simples` — opção pelo Simples Nacional e MEI (1 linha por CNPJ básico optante)

| Coluna | Descrição |
|---|---|
| `cnpj_basico` | Raiz do CNPJ |
| `opcao_simples` | S Sim · N Não · vazio = outros |
| `data_opcao_simples`, `data_exclusao_simples` | Entrada e saída do Simples (`00000000` = sem data) |
| `opcao_mei` | S Sim · N Não |
| `data_opcao_mei`, `data_exclusao_mei` | Entrada e saída do MEI |

### 3.5 Tabelas de domínio (código → descrição)

| Tabela | Colunas | Uso |
|---|---|---|
| `cnaes` | `cod_cnae`, `descricao_cnae` | Atividade econômica. Base da regra de TI |
| `naturezas_juridicas` | `cod_natureza_juridica`, `descricao_natureza_juridica` | Tipo jurídico (LTDA, S.A., EI…) |
| `qualificacoes_socios` | `cod_qualificacao`, `descricao_qualificacao` | Papel do sócio ou responsável |
| `paises` | `cod_pais`, `descricao_pais` | País |
| `motivos` | `cod_motivo`, `descricao_motivo` | Motivo da situação cadastral |
| `municipios` | `cod_municipio`, `descricao_municipio` | Município (código Receita) |

---

## 4. Arquitetura de execução

### Tabelas por camada

| Camada | Tabela | Notebook | Frente |
|---|---|---|---|
| Bronze | 10 tabelas (seção 3) | `load_bronze_cnpj` | Todos |
| Silver | `dim_cnae`, `dim_motivo`, `dim_municipio`, `dim_pais`, `dim_qualificacao`, `dim_natureza_juridica` | `silver_dominios` | Todos |
| Silver | `cnpj_estabelecimentos` | `silver_estabelecimentos` | Todos |
| Silver | `cnpj_empresas` | `silver_empresas` | Demografia |
| Silver | `cnpj_socios_compliance` | `silver_socios` | Risco societário |
| Silver | `cnpj_status_cadastral` | `silver_status_cadastral_v2` | Mortalidade |
| Gold | `demografia_ti` | `gold_demografia_ti` | Demografia (Matheus) |
| Gold | `geografia_ti` | `gold_geografia_ti` | Geografia (Rafael) |
| Gold | `situacao_cadastral_ti` | `gold_situacao_cadastral_ti_v2` | Mortalidade (Helen) |
| Gold | `perfil_societario_ti` | `gold_risco_societario` | Risco societário (Ives) |
| Controle | `silver.dq_resultados` | todos | Histórico de quality checks |
| Controle | `silver.log_execucoes` | todos | Tempo de cada etapa (base da comparação de performance) |

**Universo de TI:** matrizes com CNAE principal começando com `62` ou `631` (`silver.dim_cnae.flag_ti`). Todas as golds usam o mesmo universo, então os números entre frentes batem.

### Ordem de execução (DAG do Job)

```
load_bronze_cnpj
      │
silver_dominios
      ├──────────────────────┬────────────────────┐
silver_estabelecimentos   silver_empresas     silver_socios
      │                      │                    │
      ├── gold_geografia_ti  │                    │
      ├── gold_demografia_ti ◄┘                   │
      ├── gold_risco_societario ◄─────────────────┘
      └── silver_status_cadastral_v2 ── gold_situacao_cadastral_ti_v2
```

### Padrões do pipeline

- **Parâmetro único:** `dt_referencia` (`yyyy-MM-dd`, último dia do mês da publicação). Vem do widget ou do parâmetro do Job.
- **Particionamento por `dt_referencia`:** reprocessar um mês substitui só aquela partição (`replaceWhere`). Os outros meses ficam intactos.
- **Quality checks antes de gravar (silver e gold):** check `bloqueia` interrompe o notebook e a tabela do mês não é escrita. Check `alerta` só registra. Na bronze, os checks rodam depois da gravação (lendo a Delta, bem mais rápido que reler os CSVs). Se falharem, o Job para antes da silver.
- **Código compartilhado:** `utils_pipeline` (checks, gravação, cronômetro), incluído em cada notebook com `%run`.

---

## 5. Como rodar

### Primeira vez (dono do catálogo `workspace`)

1. Rodar o `00_setup` com o widget `executar_migracao = sim`. Ele cria os schemas, dá as permissões e apaga as tabelas no formato antigo. Todas são reconstruídas a partir dos Volumes; nenhum dado se perde.
2. Confirmar no `load_bronze_cnpj` os Volumes de `motivos` e `municipios` (marcados `CONFIRMAR`).
3. Rodar o Job abaixo com `dt_referencia = 2026-09-30`.

### Job mensal (Databricks → Jobs & Pipelines → Create job)

1. **Parâmetro do Job:** `dt_referencia` = `2026-09-30`. O Databricks repassa o valor ao widget de mesmo nome em cada notebook.
2. **Uma task por notebook**, tipo *Notebook*, origem *Git provider* (este repositório, branch `main`), compute *Serverless*.
3. **Dependências** (*Depends on*) conforme o DAG acima.
4. **Agendamento:** mensal, alguns dias depois da publicação da Receita (ex.: dia 15). Deixar **pausado** durante o trabalho e disparar com *Run now*.
5. **Notificação** de falha por e-mail para o grupo.

### Mês novo
1. Copiar os CSVs do mês para os Volumes.
2. Rodar o Job com o novo `dt_referencia`.

---

## 6. Evidências para a entrega (item 3.1)

| # | Print | Onde |
|---|---|---|
| 1 | Job com todas as tasks verdes (grafo do DAG) | Jobs → execução |
| 2 | Checks do mês | `SELECT * FROM workspace.silver.dq_resultados WHERE dt_referencia = '2026-09-30' ORDER BY tabela` |
| 3 | **Check falhando dentro do notebook** | `silver_estabelecimentos`, seção 4.1 (duplicata + situação `99` → gravação bloqueada) |
| 4 | **Check falhando no Job** | *Run now with different parameters* só da task `silver_estabelecimentos` com `dt_referencia = 2026-08-31` (bronze vazia → "tabela não vazia" = FALHA → task vermelha, golds não rodam) |
| 5 | Consultas finais na gold | Seção "Consultas de negócio" de cada `gold_*` |
| 6 | Tempo por etapa | `SELECT * FROM workspace.silver.log_execucoes ORDER BY dt_execucao` |
| 7 | Lineage no Unity Catalog | Catalog Explorer → `gold.demografia_ti` → aba *Lineage* |

---

## 7. Arquivos legados (não fazem parte do pipeline)

| Arquivo | Situação |
|---|---|
| `fluxo_tb_cad_receita_risco.py` | Exploração inicial, substituída pelo `load_bronze_cnpj` (tinha schema de sócios trocado pelo de empresas). Pode ser removido; o texto da problemática está neste README |
| `silver_cnpj_status_cadastral.py`, `gold_situ_cadastral_TI.py` | Versão original da frente de Mortalidade. A proposta `*_v2` corrige o filtro de `cod_pais`, a escolha do estabelecimento e a regra de idade. A frente decide qual fica |
