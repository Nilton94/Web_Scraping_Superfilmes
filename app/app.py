from utils.utils_movies_metadata import GetMovieMetadata
from utils.utils_series_metadata import GetSeriesMetadata
import asyncio
import datetime, pytz
import pandas as pd
import streamlit as st 
from IPython.display import display, HTML

# CONFIGS
st.set_page_config(
        page_title = "Filmes e Séries Disponíveis no Superfilmes",
        layout = 'wide',
        menu_items = {
            'about': 'App simples, com objetivos educativos, para obter lista de filmes e séries disponíveis no site superfilmes. Criado por josenilton1878@gmail.com'
        }

    )

pd.set_option('display.max_colwidth', 1000)

# WIDGETS
st.sidebar.image(
    image = 'https://variety.com/wp-content/uploads/2023/04/movie-theater-placeholder.jpg?w=1000'
    # image = 'https://superfilmes.red/image/poster/330490/200486714.jpg'
)

st.sidebar.markdown('---')

st.sidebar.selectbox(
    label = 'Categoria',
    options = ['series', 'filmes'],
    key = 'categorias'
)

st.sidebar.button(
    label = 'Atualizar',
    key = 'atualizar'
)

if 'dataframe' not in st.session_state:
    st.session_state['dataframe'] = []

# BASE
if st.session_state.atualizar:
    # DADOS
    inicio = datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond = 0, tzinfo = None)

    if st.session_state.categorias == 'series':
        dados = asyncio.run(GetSeriesMetadata(st.session_state.categorias).get_series_metadata())
    else:
        dados = asyncio.run(GetMovieMetadata(st.session_state.categorias).get_movie_metadata())

    fim = datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond = 0, tzinfo = None)
    
    # SIDEBAR COM DURACAO
    st.sidebar.markdown('---')
    try:
        st.sidebar.write(f"<b>Início</b>: {inicio}", unsafe_allow_html = True)
        st.sidebar.write(f"<b>Fim</b>: {fim}", unsafe_allow_html = True)
        st.sidebar.write(f"<b>Duração</b>: {fim - inicio}", unsafe_allow_html = True)
    except:
        pass

    df = pd.DataFrame(dados[0])

    st.session_state['dataframe'].append(df.to_dict(orient='list'))
    
    # FILTROS
    if st.session_state.categorias == 'series':
        col1, col2, col3, col4 = st.columns(4)
        
        col1.multiselect(
            label = 'Gênero',
            options = df.genero.unique(),
            key = 'genero'
        )
        col2.multiselect(
            label = 'Ano de Lançamento',
            options = df.ano_raw.sort_values().unique(),
            key = 'ano'
        )
        col3.multiselect(
            label = 'Temporadas',
            options = df.temporadas.sort_values().unique(),
            key = 'temporadas'
        )
        col4.multiselect(
            label = 'Episódios',
            options = df.episodios.sort_values().unique(),
            key = 'Episódios'
        )
    else:
        col1, col2, col3, col4 = st.columns(4)

        col1.multiselect(
            label = 'Gênero',
            options = df.genero.unique(),
            key = 'genero'
        )
        col2.multiselect(
            label = 'Ano de Lançamento',
            options = df.ano_raw.sort_values().unique(),
            key = 'ano'
        )
        col3.multiselect(
            label = 'Nota',
            options = df.nota_float.sort_values().unique(),
            key = 'nota_float'
        )
        col4.multiselect(
            label = 'Legendas',
            options = df.legendas.sort_values().unique(),
            key = 'legendas'
        )

    # st.session_state['dataframe'].append(df.to_dict(orient='list'))

    # df = st.session_state['dataframe'][-1]

    if st.session_state.categorias == 'series':
        df_final = df[['image', 'name', 'url', 'description', 'genero', 'episodios', 'temporadas', 'ano_lancamento', 'data_extracao']].sort_values('ano_lancamento', ascending = False, ignore_index = True)
    else:
        df_final = df[['image', 'name', 'url', 'description', 'genero', 'nota_float', 'legendas', 'ano_lancamento', 'data_extracao']].sort_values('nota_float', ascending = False, ignore_index = True)
    
    st.data_editor(
        data = df_final, 
        use_container_width = True,
        column_config = {
            'url': st.column_config.LinkColumn(
                label = 'url',
                disabled = True,
                display_text = 'Link'
            ),
            'image': st.column_config.ImageColumn(
                label = 'logo',
                width = 'medium'
            )
        },
        height = 600,
        hide_index = True
    )

    # st.write(
    #     df_final.to_html(escape = False),
    #     unsafe_allow_html=True
    # )
    # display(
    #     HTML(
    #         df_final.to_html(escape = False)
    #     )
    # )