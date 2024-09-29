from utils.utils_movies_metadata import GetMovieMetadata
from utils.utils_series_metadata import GetSeriesMetadata
from utils.utils_movies import GetMovieData
from utils.utils_series import GetSeriesData
import asyncio
import datetime, pytz
import requests
from bs4 import BeautifulSoup
import pandas as pd
import streamlit as st 
import random
from utils.utils_streamlit import get_config, get_widgets, get_data, get_filters, apply_filters, get_final_dataframe, get_duration

# CONFIGS
get_config()

# WIDGETS
get_widgets()

# BASE
# get_data()

# SIDEBAR COM DURACAO
# get_duration()

# FILTROS
# try:
#     if st.session_state.atualizar or (len(st.session_state['dataframe_base']) > 0 and st.session_state.categorias in list(set(st.session_state['dataframe_base'][0]['categoria']))):

#         df_base_st = pd.DataFrame(st.session_state['dataframe_base'][0])

#         get_filters(df_base_st)

#         df_filtered = apply_filters(df_base_st)

#         get_final_dataframe(df_base_st_filt = df_filtered)

#     else:
#         st.write('Selecione uma categoria!')
# except:
#     pass

# if st.session_state.limpar_cache:
#     st.cache_data.clear()



# TESTES 
def get_urls_sync(url):
        
        series = []
        proxies_list = [
            {"http": "socks4://45.234.100.102:1080", "https": "socks4://45.234.100.102:1080"},
            {"http": "http://209.13.186.20:80", "https": "http://209.13.186.20:80"},
            {"http": "socks4://187.44.211.118:4153", "https": "socks4://187.44.211.118:4153"},
            {"http": "http://200.150.65.75:3109", "https": "http://200.150.65.75:3109"},
            {"http": "http://190.69.157.215:999", "https": "http://190.69.157.215:999"},
            {"http": "http://181.209.125.126:999", "https": "http://181.209.125.126:999"},
            {"http": "http://181.10.160.154:8080", "https": "http://181.10.160.154:8080"},
            {"http": "http://170.84.146.176:8099", "https": "http://170.84.146.176:8099"},
            {"http": "http://181.114.225.184:8080", "https": "http://181.114.225.184:8080"},
            {"http": "http://131.100.51.211:999", "https": "http://131.100.51.211:999"},
            {"http": "http://187.102.216.196:999", "https": "http://187.102.216.196:999"},
            {"http": "http://206.42.19.56:8080", "https": "http://206.42.19.56:8080"},
            {"http": "http://191.102.248.9:8084", "https": "http://191.102.248.9:8084"},
            {"http": "http://186.0.144.141:9595", "https": "http://186.0.144.141:9595"},
            {"http": "http://177.93.45.225:999", "https": "http://177.93.45.225:999"},
            {"http": "http://191.97.96.208:8080", "https": "http://191.97.96.208:8080"},
            {"http": "socks4://200.254.221.146:3128", "https": "socks4://200.254.221.146:3128"},
            {"http": "http://138.121.161.85:8097", "https": "http://138.121.161.85:8097"},
            {"http": "http://190.192.45.168:3128", "https": "http://190.192.45.168:3128"},
            {"http": "http://201.251.61.143:8080", "https": "http://201.251.61.143:8080"},
            {"http": "http://45.179.200.113:999", "https": "http://45.179.200.113:999"},
            {"http": "http://191.252.113.131:80", "https": "http://191.252.113.131:80"},
            {"http": "http://191.102.254.12:8085", "https": "http://191.102.254.12:8085"},
            {"http": "http://191.242.126.94:8080", "https": "http://191.242.126.94:8080"},
            {"http": "http://45.188.156.217:8088", "https": "http://45.188.156.217:8088"},
            {"http": "http://190.103.177.131:80", "https": "http://190.103.177.131:80"},
            {"http": "http://181.114.62.1:8085", "https": "http://181.114.62.1:8085"},
            {"http": "http://131.100.48.125:999", "https": "http://131.100.48.125:999"},
            {"http": "http://181.143.181.34:8080", "https": "http://181.143.181.34:8080"},
            {"http": "http://186.96.97.203:999", "https": "http://186.96.97.203:999"},
            {"http": "http://191.7.196.128:8080", "https": "http://191.7.196.128:8080"},
            {"http": "http://201.91.82.155:3128", "https": "http://201.91.82.155:3128"},
            {"http": "http://201.62.125.142:8080", "https": "http://201.62.125.142:8080"},
            {"http": "http://200.41.170.210:11201", "https": "http://200.41.170.210:11201"},
            {"http": "http://200.61.16.80:8080", "https": "http://200.61.16.80:8080"},
            {"http": "http://189.51.123.7:80", "https": "http://189.51.123.7:80"},
            {"http": "http://170.81.171.189:8089", "https": "http://170.81.171.189:8089"},
            {"http": "http://177.81.30.222:8080", "https": "http://177.81.30.222:8080"},
            {"http": "socks4://200.152.74.81:5678", "https": "socks4://200.152.74.81:5678"},
            {"http": "socks4://186.212.164.139:1337", "https": "socks4://186.212.164.139:1337"},
            {"http": "socks4://170.81.108.46:4153", "https": "socks4://170.81.108.46:4153"},
            {"http": "socks4://191.102.251.254:4153", "https": "socks4://191.102.251.254:4153"},
            {"http": "socks4://186.224.225.30:42648", "https": "socks4://186.224.225.30:42648"},
            {"http": "socks4://191.102.82.83:4153", "https": "socks4://191.102.82.83:4153"},
            {"http": "socks4://200.122.92.211:5678", "https": "socks4://200.122.92.211:5678"},
            {"http": "socks4://181.15.154.156:52033", "https": "socks4://181.15.154.156:52033"},
            {"http": "socks4://189.50.138.10:5678", "https": "socks4://189.50.138.10:5678"},
            {"http": "socks4://200.106.184.21:999", "https": "socks4://200.106.184.21:999"},
            {"http": "socks4://200.110.173.17:999", "https": "socks4://200.110.173.17:999"},
            {"http": "socks4://201.216.239.162:1080", "https": "socks4://201.216.239.162:1080"},
            {"http": "socks4://38.56.23.33:999", "https": "socks4://38.56.23.33:999"},
            {"http": "socks4://168.232.60.62:5678", "https": "socks4://168.232.60.62:5678"},
            {"http": "http://185.236.182.3:18080", "https": "http://185.236.182.3:18080"},
            {"http": "socks4://206.42.40.0:5678", "https": "socks4://206.42.40.0:5678"},
            {"http": "http://207.230.8.1:999", "https": "http://207.230.8.1:999"},
            {"http": "http://209.14.118.161:999", "https": "http://209.14.118.161:999"},
            {"http": "http://192.140.42.83:31511", "https": "http://192.140.42.83:31511"},
            {"http": "socks4://177.106.0.129:3128", "https": "socks4://177.106.0.129:3128"},
            {"http": "http://187.94.220.85:8080", "https": "http://187.94.220.85:8080"},
            {"http": "socks4://179.108.181.73:4153", "https": "socks4://179.108.181.73:4153"},
            {"http": "socks4://186.96.124.242:4153", "https": "socks4://186.96.124.242:4153"},
            {"http": "socks4://179.125.172.177:4153", "https": "socks4://179.125.172.177:4153"},
            {"http": "http://189.50.9.30:8080", "https": "http://189.50.9.30:8080"},
            {"http": "http://186.250.29.225:8080", "https": "http://186.250.29.225:8080"}
        ]
        proxy = random.choice(proxies_list)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        }
        r = requests.get(url, headers = headers, proxies=proxy)
        soup = BeautifulSoup(r.text, 'html.parser')

        for item in soup.select('.zmovo-video-item-box'):
            name = item.select_one('.zmovo-v-box-content a.glow').text.strip()
            url = item.select_one('.zmovo-v-box-content a.glow')['href']
            genre = item.select_one('.zmovo-v-tag span').text.strip()
            logo = item.select_one('img')['data-src']
            duracao = item.find('div', 'movie-time').text
            

            series.append(
                {
                    'uuid': str((name.lower())),
                    'name': name, 
                    'url': url, 
                    'genre': genre, 
                    'duracao': duracao,
                    'logo': logo,
                    'data_extracao': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).strftime('%Y-%m-%d %H:%M:%S')),
                    'ano_extracao': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).year),
                    'mes_extracao': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).month),
                    'dia_extracao': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).day)
                }
            )
        
        return series, r.status_code, r.reason, r.text

if st.session_state.atualizar:

    if st.session_state.categorias == 'series':
        # dados = asyncio.run(GetSeriesMetadata(st.session_state.categorias).get_series_metadata())
        # dados = GetSeriesData(st.session_state.categorias).get_series_data_sync()
        dados, status, reason, text = get_urls_sync(url = 'https://superfilmes.green/series/1')
        # dados = await GetSeriesMetadata(st.session_state.categorias).get_series_metadata()
    else:
        # dados = asyncio.run(GetMovieMetadata(st.session_state.categorias).get_movie_metadata())
        dados = GetMovieData(st.session_state.categorias).get_movie_data_sync()
        # dados = await GetMovieMetadata(st.session_state.categorias).get_movie_metadata()

    st.write(status)
    st.write(reason)
    st.write(text)
    st.dataframe(dados)


response = requests.get('https://httpbin.org/ip')
st.write(response.json())