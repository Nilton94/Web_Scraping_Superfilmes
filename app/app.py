from utils.utils_movies_metadata import GetMovieMetadata
from utils.utils_series_metadata import GetSeriesMetadata
from utils.utils_movies import GetMovieData
from utils.utils_series import GetSeriesData
import asyncio
import datetime, pytz
import pandas as pd
import streamlit as st 
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

if st.session_state.atualizar:

    if st.session_state.categorias == 'series':
        # dados = asyncio.run(GetSeriesMetadata(st.session_state.categorias).get_series_metadata())
        # dados = GetSeriesData(st.session_state.categorias).get_series_data_sync()
        dados = GetSeriesData(st.session_state.categorias).get_urls_sync(url = 'https://superfilmes.green/series/1')
        # dados = await GetSeriesMetadata(st.session_state.categorias).get_series_metadata()
    else:
        # dados = asyncio.run(GetMovieMetadata(st.session_state.categorias).get_movie_metadata())
        dados = GetMovieData(st.session_state.categorias).get_movie_data_sync()
        # dados = await GetMovieMetadata(st.session_state.categorias).get_movie_metadata()

    st.dataframe(dados)