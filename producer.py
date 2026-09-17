"""
Producer: vai buscar o preço do Bitcoin e Ethereum à API da CoinGecko
a cada 30 segundos, e envia essa informação para o Kafka (topic: crypto_prices).
"""

import json
import time
import requests
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "crypto_prices"
INTERVALO_SEGUNDOS = 30

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def buscar_precos():
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {"ids": "bitcoin,ethereum", "vs_currencies": "usd"}
    resposta = requests.get(url, params=params)
    return resposta.json()


def main():
    print("Producer iniciado. A enviar preços para o Kafka...")

    while True:
        try:
            dados = buscar_precos()
            mensagem = {
                "bitcoin_usd": dados["bitcoin"]["usd"],
                "ethereum_usd": dados["ethereum"]["usd"],
                "timestamp": time.time()
            }
            producer.send(KAFKA_TOPIC, value=mensagem)
            print(f"Enviado: {mensagem}")

        except (KeyError, requests.exceptions.RequestException) as e:
            print(f"Aviso: falha temporária ao obter/enviar preços ({e}). A tentar de novo no próximo ciclo.")

        time.sleep(INTERVALO_SEGUNDOS)


if __name__ == "__main__":
    main()