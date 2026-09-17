# Projeto 2 — Pipeline de Streaming (Kafka + Spark Structured Streaming)

Pipeline de dados em tempo real que recolhe preços de criptomoedas, processa-os continuamente com agregações por janela de tempo, e apresenta os resultados num dashboard atualizado automaticamente.

Este é o segundo projeto de um portfólio de 3 projetos ("mini data stack"), que juntos cobrem processamento batch, streaming e IA generativa.

## Arquitetura

CoinGecko API → Producer (Python) → Kafka → Spark Structured Streaming
│
(windowing + watermarking)
│
▼
Ficheiros Parquet
│
▼
Dashboard (Streamlit)


**Fluxo de dados:**
1. O **producer** vai buscar o preço do Bitcoin e Ethereum à API da CoinGecko a cada 30 segundos, e publica-os num topic do Kafka (`crypto_prices`).
2. O **Kafka** funciona como intermediário entre o producer e o consumer, permitindo que ambos operem de forma independente.
3. O **consumer**, em Spark Structured Streaming, lê continuamente do Kafka, agrupa os dados em janelas de 1 minuto, e calcula a média do preço dentro de cada janela.
4. O resultado é persistido em **ficheiros Parquet**, que funcionam aqui como o "warehouse" local do projeto.
5. Um **dashboard em Streamlit** lê esses ficheiros e apresenta a evolução dos preços em tempo real, com atualização automática a cada 10 segundos.

## Tecnologias

- **Apache Kafka** (via Docker) — sistema de mensagens para ingestão de dados em tempo real
- **Spark Structured Streaming** — processamento contínuo com agregações por janela de tempo (windowing) e tratamento de dados atrasados (watermarking)
- **Python** (`kafka-python`, `requests`) — producer que consome a API externa
- **Parquet** — formato de armazenamento colunar para os dados agregados
- **Streamlit** — dashboard interativo com atualização automática

## Como correr o projeto

### Pré-requisitos
- Docker e Docker Compose
- Python 3.11+

### Passos

1. Arrancar o Kafka via Docker:
docker compose up -d

2. Criar e ativar o ambiente virtual, e instalar dependências:
python -m venv venv
venv\Scripts\Activate.ps1
pip install -r requirements.txt

3. Correr o producer (num terminal):
python producer.py

4. Correr o consumer (noutro terminal):
python consumer.py

5. Correr o dashboard (noutro terminal):

streamlit run dashboard.py

O dashboard abre automaticamente no browser (`http://localhost:8501`).

## Decisões técnicas

- **Watermarking de 30 segundos**: dado que o producer envia dados a cada 30 segundos, este valor garante alguma tolerância a atrasos de rede sem manter estado indefinidamente em memória.
- **Persistência em Parquet local em vez de BigQuery**: por indisponibilidade de credenciais neste momento, optou-se por Parquet local, que mantém a mesma lógica de agregação e escrita — a migração para um data warehouse na cloud é uma extensão natural e não exige alterações à lógica de processamento, apenas ao destino da escrita (`writeStream.format(...)`).
- **Tratamento de falhas no producer**: chamadas à API da CoinGecko podem falhar por rate limiting; o producer captura esses erros e continua a correr, em vez de terminar — um comportamento essencial num sistema pensado para correr continuamente.

## Próximos passos possíveis

- Migrar a persistência para um data warehouse na cloud (BigQuery, ou equivalente)
- Adicionar deteção de variações bruscas de preço (alertas)
- Testes automatizados para a lógica de agregação