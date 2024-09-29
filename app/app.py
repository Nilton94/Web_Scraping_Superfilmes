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
import cloudscraper
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
        scraper = cloudscraper.create_scraper()
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        }
        r = scraper.get(url, headers = headers)
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