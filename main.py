import json
from binance.client import Client
from binance.exceptions import BinanceAPIException
import pandas as pd
# import numpy as np
import schedule
import time
# import requests
from datetime import timezone, datetime, timedelta
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception
from http.client import RemoteDisconnected
from requests.exceptions import ReadTimeout, Timeout, ConnectionError
# import pytz

# Defina suas chaves de API
api_key = 'WBAZFgSiwagZKRJQzammtFBUpXQ03meDioFib8Ynj6EhbaaEGu6fmBXgBsXSisbo'
api_secret = 'nNGkz5PdC7lNw7Cnt1TMJwDcunWr5szsW5Ly1H89fLuXbCmvjPmHRc94nuOOaS8T'

# Configurar o cliente da Binance
client = Client(api_key, api_secret, testnet=True)
client.API_URL = 'https://testnet.binance.vision/api'

def is_retryable_exception(exception):
    if isinstance(exception, BinanceAPIException):
        print(f"BinanceAPIException: {exception.message}")
        return True
    if isinstance(exception, ConnectionError):
        print(f"ConnectionError: {exception}")
        return True
    if isinstance(exception, RemoteDisconnected):
        print(f"RemoteDisconnected: {exception}")
        return True
    if isinstance(exception, ReadTimeout):
        print(f"ReadTimeout: {exception}")
        return True
    if isinstance(exception, Timeout):
        print(f"Timeout: {exception}")
        return True
    if hasattr(exception, 'response') and exception.response.status_code == 502:
        print("502 Bad Gateway")
        return True
    return False

@retry(stop=stop_after_attempt(10),
       wait=wait_exponential(multiplier=1, min=4, max=60),
       retry=retry_if_exception(is_retryable_exception))
def get_historical_data(symbol, interval, start_str):

    klines = client.get_historical_klines(symbol, interval, start_str)

    df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time',
                                       'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume',
                                       'taker_buy_quote_asset_volume', 'ignore'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)
    df['close'] = df['close'].astype(float)

    return df

@retry(stop=stop_after_attempt(10),
       wait=wait_exponential(multiplier=1, min=4, max=60),
       retry=retry_if_exception(is_retryable_exception))
def my_stategy(ticker, items):

    print(f"{ticker}")

    # Variávei para criar JSON para mostrar na aplicação Web
    item = {}

    item['ticker'] = ticker
    # Para ordenacao
    item['variacao'] = ""
    item['status'] = ""
    item['ult_atualizacao'] = datetime.now().strftime('%d/%m %H:%M')

    # Compra Porcentagem de queda para compra
    porc_queda = -0.03
    # Venda Take Profit, Stop Loss
    tp = 0.02
    sl = -0.03

    # Variável para indicar se está comprado no ativo
    comprado = None
    # Variável para indicar se está vendido
    datetime_venda = None
    # Variáveis para o valor acumulado
    acc = None

    # CSV para operações
    # datetime | ativo | operação (compra ou venda) | valor | acc
    columns = ['datetime', 'ativo', 'operacao', 'valor', 'acc']

    # Caminho completo para o arquivo CSV no Google Drive
    file_path = f"C:/Users/toon_/PycharmProjects/BinanceBot/venv/Files/{ticker}.csv"
    dados_temp = None
    try:
        dados_temp = pd.read_csv(file_path)
        # LOG:
        # print(f"Arquivo '{file_path}' encontrado.")
    except FileNotFoundError:
        # Se não tem arquivo, significa que não tem compra
        comprado = False
        # Criar novo arquivo
        dados_temp = pd.DataFrame(columns=columns)
        dados_temp.to_csv(file_path, index=False)
        # LOG:
        print(f"Arquivo '{file_path}' NÃO encontrado.")

    # Verificar se tem dados no arquivo
    if len(dados_temp) > 0:

        # LOG:
        # print(f"Arquivo '{file_path}' com dados.")

        # Verificar se o último registro de operações é uma compra, ou seja, se a colunao operacao é C.
        if dados_temp['operacao'].iloc[-1] == 'C':
            comprado = True
            # Recuperar o valor de compra para usar na próxima etapa
            valor_compra = float(dados_temp['valor'].iloc[-1])
            # Recuperar valor acumulado para cálculo do próximo
            acc = float(dados_temp['acc'].iloc[-1])
            # LOG:
            # print(f"A última operação foi COMPRA.")

        # Caso não esteja comprado, estará vendido V. Neste caso, recuperar o datetime para usar na próxima etapa
        else:
            comprado = False
            datetime_venda = dados_temp['datetime'].iloc[-1]
            # Recuperar valor acumulado para cálculo do próximo
            acc = float(dados_temp['acc'].iloc[-1])
            # LOG:
            # print(f"A última operação foi VENDA.")

        # Dado para json da aplicação Web
        # Converter a string em um objeto datetime assumindo que está em UTC
        # data_utc = datetime.strptime(dados_temp['datetime'].iloc[-1], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        try:
            # Primeiro, tenta o formato com data e hora
            data_utc = datetime.strptime(dados_temp['datetime'].iloc[-1], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except ValueError:
            # Se falhar, tenta o formato apenas com a data
            data_utc = datetime.strptime(dados_temp['datetime'].iloc[-1], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        # Converter para o timezone UTC-3
        data_utc_minus_3 = data_utc.astimezone(timezone(timedelta(hours=-3)))
        item['ts_operacao'] = data_utc_minus_3.strftime('%d/%m %H:%M')
        item['num_operacoes'] = int(len(dados_temp)/2)

    # Se não tem dados no arquivo, consequentemente não tem compra ou venda
    else:
        comprado = False
        # Definir valor inicial para acc (Usar 100 para facilitar a visão da % que o valor está)
        acc = 100.0
        # LOG:
        # print(f"Arquivo '{file_path}' SEM DADOS.")

    # ########################################################
    # VENDA
    # if: Se estiver comprado no ativo, verificar se tem venda
    if comprado:

        # LOG:
        # print('Está Comprado')

        # Recuperar o preço atual
        symbol = client.get_symbol_ticker(symbol=ticker)

        preco_atual = float(symbol['price'])

        # LOG:
        # print(f"Preço atual do {ticker}: {symbol['price']}")
        tmp = (preco_atual/valor_compra - 1) * 100
        print(f"Compra/Atual ({ticker}): {valor_compra}/{symbol['price']} #({tmp:.1f}%)#")
        item['status'] = "comprado"
        item['compra'] = valor_compra
        item['atual'] = preco_atual
        item['variacao'] = tmp

        # Verificar se foi possível recuperar o preço atual
        if preco_atual is None:
            # LOG:
            print(f"Não foi possível recuperar o preço atual do ativo {ticker}.")

        else:

            # Se o preço atual atingiu a % de tp ou stop loss, realizar a venda e registrar a operação no CSV
            if (preco_atual / valor_compra - 1) > tp or (preco_atual / valor_compra - 1) < sl:

                # TODO: Realizar a venda

                # Registrar no CSV de operacoes
                # datetime | ativo | operação (compra ou venda) | valor | Acc (Valor acumulado a partir de 100)
                new_df = pd.DataFrame()
                # Garantir que a data é UTC universal
                new_df['datetime'] = [datetime.now().astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')]
                new_df['ativo'] = [ticker]
                new_df['operacao'] = ['V']
                new_df['valor'] = [preco_atual]
                new_df['acc'] = [acc+acc*(preco_atual / valor_compra - 1)]
                # Verificar se o arquivo já existe
                try:
                    # Se o arquivo existe, carregar o CSV existente e concatenar os novos dados
                    existing_df = pd.read_csv(file_path)
                    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                except FileNotFoundError:
                    # Se o arquivo não existe, usar apenas os novos dados
                    combined_df = new_df

                # Salvar os dados combinados de volta no CSV
                combined_df.to_csv(file_path, index=False)

                # comprado = False
                # LOG:
                tmp = preco_atual / valor_compra - 1
                print(f"Venda realizada: {preco_atual}/{valor_compra} : {tmp}")
                print("#################################################")

    # ####################################################################
    # COMPRA
    # else: Se não estiver comprado no ativo, verificar se tem compra
    else:
        # LOG:
        # print(f"Arquivo '{file_path}' não está comprado.")

        # Comprar se tiver queda de x% desde o último topo ou desde a última venda (loss ou tp) e registrar a operação
        # PROBLEMA: Comprou SKLUSDT sem considerar a última venda, verificar!

        # Verificar nas operações quando, e se, houve a última venda (a partir de datetime_venda), caso contrário, será usado o tempo de 1 dia
        if datetime_venda is not None:
            start_str = datetime_venda
            # LOG:
            print(f"Última venda: {datetime_venda}")
        else:
            start_str = "1 day ago UTC"
            # LOG:
            print(f"Última venda: NENHUMA, padrão de recuperação de dados {start_str}")

        # Recuperar dados atualizados desde a última venda ou de start_str caso não tenha ocorrência de vendas
        dados = get_historical_data(ticker, Client.KLINE_INTERVAL_5MINUTE, start_str=start_str)

        # LOG:
        # print(f"start_str: {start_str}")

        # Variável para manter a informação do preço mais atual do ativo e identificar se há variação para realizar a compra ou a venda
        preco_inicial = None
        preco = None

        # Navegar no df de maneira inversa
        for index, row in dados.iloc[::-1].iterrows():

            # Recuperar o preço atual do ativo
            preco = row['close']

            item['status'] = "vendido"

            # LOG:
            # print(f"{preco}/{preco_inicial}")

            # Se o preço inicial for None, ou seja, é o valor mais atualizado do ativo
            if preco_inicial is None:

                # Armazenar o preço inicial da variável
                preco_inicial = preco
                datetime_inicial = index

            # Caso contrário, se o preço inicial não for None
            else:

                # Condição para parar de executar a rotina de compra do ativo
                # Se o preco atingir um valor abaixo do SL do preco_inicial, o laço de repetição (for) deve ser encerrado pois não há sinal de compra.
                if preco < (preco_inicial * (1 + sl)):
                    # LOG:
                    print(f"Não há sinal de compra desde o último topo. Preço: {preco}/{preco_inicial}")
                    break

                # Verificar se atingiu a % de queda para compra e não está comprado
                if not comprado and ((preco_inicial / preco - 1) < porc_queda):

                    # TODO: Fazer a compra

                    # Registrar no CSV de operacoes
                    # datetime | ativo | operação (compra ou venda) | valor | acc
                    new_df = pd.DataFrame()
                    new_df['datetime'] = [datetime_inicial]
                    new_df['ativo'] = [ticker]
                    new_df['operacao'] = ['C']
                    new_df['valor'] = [preco_inicial]
                    new_df['acc'] = [acc]
                    # Verificar se o arquivo já existe
                    try:
                        # Se o arquivo existe, carregar o CSV existente e concatenar os novos dados
                        existing_df = pd.read_csv(file_path)
                        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                    except FileNotFoundError:
                        # Se o arquivo não existe, usar apenas os novos dados
                        combined_df = new_df

                    # Salvar os dados combinados de volta no CSV
                    combined_df.to_csv(file_path, index=False)

                    comprado = True
                    # LOG:
                    tmp = preco_inicial / preco - 1
                    print(f"Compra realizada: {preco}/{preco_inicial} - {tmp}")
                    print("#################################################")

        print(f"P Ini: {preco_inicial} e P mais antigo: {preco} ACC: {acc:.2f}")

    item['acumulado'] = acc
    items.append(item)
    return acc

def place_order(symbol, side, quantity):
    try:
        order = client.create_order(
            symbol=symbol,
            side=side,
            type='MARKET',
            quantity=quantity
        )
        print(order)
    except Exception as e:
        print(f"An exception occurred - {e}")


def run_bot():
    # symbols = [
    #     'ALPINEUSDT',
    #     'POLYXUSDT',
    #     'REEFUSDT',
    #     'ROSEUSDT',
    #     'SKLUSDT',
    #     'AAVEUSDT',
    #     'STORJUSDT',
    #     'ICPUSDT',
    #     'SPELLUSDT',
    #     'SUIUSDT'
    # ]
    symbols = [
        'ALPINEUSDT', 'SKLUSDT', 'REEFUSDT', 'AAVEUSDT', 'SPELLUSDT', 'SUIUSDT', 'POLYXUSDT', 'STORJUSDT',
        'GLMUSDT', 'ROSEUSDT', 'TLMUSDT', 'FLOWUSDT', 'ZRXUSDT', 'LPTUSDT', 'FILUSDT', 'ONEUSDT',
        'LOKAUSDT', 'GALAUSDT', 'KDAUSDT', 'PORTOUSDT', 'UNIUSDT', 'ONTUSDT', 'NMRUSDT', 'SANTOSUSDT', 'API3USDT',
        'MATICUSDT', 'ARBUSDT', 'VTHOUSDT', 'ENSUSDT', 'ZECUSDT', 'LTCUSDT', 'IOSTUSDT', 'RVNUSDT', 'SLPUSDT',
        'MASKUSDT', 'DOGEUSDT', 'OXTUSDT', 'BALUSDT', 'THETAUSDT', 'APEUSDT', 'EGLDUSDT', 'DASHUSDT', 'IOTAUSDT',
        'SUSHIUSDT', 'AXLUSDT', 'KNCUSDT', '1INCHUSDT', 'SNXUSDT', 'XECUSDT', 'VETUSDT', 'KSMUSDT', 'CHZUSDT',
        'ZILUSDT', 'ALICEUSDT', 'KAVAUSDT', 'SYSUSDT', 'ALGOUSDT', 'LSKUSDT', 'ONGUSDT', 'CTSIUSDT', 'RLCUSDT',
        'ICPUSDT', 'DARUSDT', 'LRCUSDT', 'ASTRUSDT', 'AXSUSDT', 'WAXPUSDT', 'TFUELUSDT', 'LOOMUSDT', 'LINKUSDT',
        'CLVUSDT', 'RENUSDT', 'COMPUSDT', 'SANDUSDT', 'STGUSDT', 'PROMUSDT', 'ADAUSDT', 'ZENUSDT', 'SOLUSDT', 'OGNUSDT',
        'VITEUSDT', 'DGBUSDT', 'ETHUSDT', 'BATUSDT', 'BTCUSDT', 'GRTUSDT', 'BNTUSDT', 'TRXUSDT', 'ETCUSDT', 'CELOUSDT',
        'YFIUSDT', 'EOSUSDT', 'ACHUSDT', 'LAZIOUSDT', 'COTIUSDT', 'BICOUSDT', 'ENJUSDT', 'REQUSDT', 'BNBUSDT',
        'BLURUSDT', 'JASMYUSDT', 'FLOKIUSDT', 'LTOUSDT', 'RADUSDT', 'ILVUSDT', 'PAXGUSDT', 'TUSDUSDT',
        'ANKRUSDT', 'QTUMUSDT', 'APTUSDT', 'XNOUSDT', 'CELRUSDT', 'FORTHUSDT', 'IMXUSDT', 'FLUXUSDT',
        'ADXUSDT', 'LDOUSDT', 'XTZUSDT', 'XLMUSDT', 'DIAUSDT', 'VOXELUSDT', 'FTMUSDT', 'ATOMUSDT', 'XRPUSDT', 'ICXUSDT',
        'MANAUSDT', 'BCHUSDT', 'DOTUSDT', 'MKRUSDT', 'AVAXUSDT', 'CRVUSDT', 'GTCUSDT', 'OPUSDT', 'QNTUSDT', 'RAREUSDT',
        'TUSDT', 'HBARUSDT', 'FETUSDT', 'AUDIOUSDT', 'BANDUSDT', 'PONDUSDT', 'STMXUSDT'
    ]
    # Variável para criar a média dos valores acumulados
    acc_acc = 0
    # df para criar o json para ser usado na aplicação web
    items = []
    for symbol in symbols:
        acc_acc += my_stategy(symbol, items)

    print(f"Média do Acumulado: {acc_acc / len(symbols):.2f}")

    items.sort(key=lambda x: (x['status'], x['variacao']))
    # Salvar o df como json
    data = {"items": items}
    # Criar e escrever no arquivo JSON
    with open('C:/Users/toon_/PycharmProjects/vuetify-project/vue-binancebot/public/dados_estrategia_01.json', 'w') as json_file:
        json.dump(data, json_file, indent=4)

    print("Arquivo JSON atualizado com sucesso.")


# Agendar a execução do bot a cada 1 minutos
schedule.every(1).minutes.do(run_bot)

while True:
    schedule.run_pending()
    time.sleep(1)
