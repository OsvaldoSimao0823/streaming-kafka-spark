"""
Dashboard: lê os ficheiros Parquet gerados pelo consumer e mostra
a evolução dos preços agregados em tempo real, com atualização automática.
"""

import streamlit as st
import pandas as pd
import time

OUTPUT_PATH = "output/crypto_agregado"
INTERVALO_ATUALIZACAO_SEGUNDOS = 10

st.set_page_config(page_title="Crypto Streaming Dashboard", layout="wide")

st.title("📈 Preços de Cripto em Tempo Real")
st.caption("Dados agregados por janela de 1 minuto, via Kafka + Spark Structured Streaming")

try:
    df = pd.read_parquet(OUTPUT_PATH).sort_values("inicio_janela")

    col1, col2 = st.columns(2)
    col1.metric("Última média BTC (USD)", f"${df['media_btc'].iloc[-1]:,.2f}")
    col2.metric("Última média ETH (USD)", f"${df['media_eth'].iloc[-1]:,.2f}")

    st.subheader("Bitcoin — média por minuto")
    st.line_chart(df.set_index("inicio_janela")["media_btc"])

    st.subheader("Ethereum — média por minuto")
    st.line_chart(df.set_index("inicio_janela")["media_eth"])

    st.subheader("Dados completos")
    st.dataframe(df, use_container_width=True)

except Exception as e:
    st.warning(f"Ainda não há dados suficientes, ou ocorreu um erro: {e}")

time.sleep(INTERVALO_ATUALIZACAO_SEGUNDOS)
st.rerun()