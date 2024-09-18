import aiohttp
import asyncio
import re
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from dataclasses import dataclass
from utils.utils_uuid import generate_uuid
from utils.utils_pages import GetPageNumber
from utils.utils_series import GetSeriesData
from utils.utils_images import open_image
import os
from PIL import Image
import streamlit as st
import datetime, pytz

@dataclass
class GetSeriesMetadata:
    _category: str = 'series'

    async def get_urls(self, session, url, duracao, uuid, series_name, series_url, logo):
        '''
            Returns the movie metadata for every url passed

            Args:
                session: aiohttp session
                url: url of the webpage that will be scraped
            Returns:
                list of dictionaries with html content
        '''

        async with session.get(url) as response:
            soup = BeautifulSoup(await response.text(), 'html.parser')

            # DESCRIPTION
            try:
                description = soup.find('p',{'itemprop':'description'}).text
            except:
                description = 'Null'

            # METADADOS
            try:
                dados_gerais = {
                    re.sub(r"\s*:*", "", x.find('span').text): x.find_all('a') 
                    for x in soup.find('div', 'dec-review-meta').find_all('li')
                }

                dados_gerais_final = {
                    k:', '.join([i.text for i in dados_gerais[k]]) 
                    for k,_ in dados_gerais.items()
                }

                genero = dados_gerais_final['Gênero']
                elenco = dados_gerais_final['Elenco']
                direcao = dados_gerais_final['Direção']
                ano = dados_gerais_final['Ano']
            except:
                genero = 'Null'
                elenco = 'Null'
                direcao = 'Null'
                ano = 'Null'

            # GENERO AJUSTADO
            try:
                genero = re.sub(r'\s*', '', genero).split(',')
                genero.sort()
                genero_final = ', '.join(genero)
            except:
                genero_final = 'Null'

            # Ano
            try:
                ano_int = int(ano.strip().replace(r'\n', ''))
            except:
                ano_int = None

            # Temporadas atualizadas - float
            try:
                temporadas_float = float(re.sub(r'[^\d]', '', soup.find('div', 'rowp').find('h4').text))
            except:
                temporadas_float = None
            
            # Temporadas atualizadas - string
            try:
                temporadas_raw = soup.find('div', 'rowp').find('h4').text
            except:
                temporadas_raw = None

            # LISTA TEMPORADAS
            try:
                lista_temporadas = [i.text for i in soup.find_all('button', 'accordion')]
            except:
                lista_temporadas = None

            # LEGENDAS
            try:
                dados = []
                for i in soup.find_all('div', 'mvcast-item'):
                    temporada = re.search(r'(\d*)x(\d*)', i.find('h4').text).groups()[0]
                    episodio = re.search(r'(\d*)x(\d*)', i.find('h4').text).groups()[1]
                    legendas = [l.text for l in i.find_all('a')]

                    dados.append(
                        {
                            # 'uuid': str(uuid),
                            'temporada': temporada,
                            'episodio': episodio, 
                            'legendas': legendas
                        }
                    )
            except:
                dados = []

            # Episodios
            episodios = len(dados)

            # LOGO FOLDER
            try:
                logo_folder = os.path.join(os.getcwd(), 'assets', f'{self._category}', f'{uuid}.png') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'assets', f'{self._category}', f'{uuid}.png')
                image = open_image(uuid = uuid, category = self._category)
            except:
                logo_folder = ''
                image = None

            # Base final
            metadata = {
                'uuid': str(uuid),
                'name': series_name, 
                'categoria': self._category,
                'url': series_url, 
                'url_src': f'<a href="{series_url}" target="_blank">{series_name}</a>',
                'logo': logo,
                'logo_src': f'<img src="{logo}" width="100" height="150">',
                'logo_folder': logo_folder,
                'image': image,
                'description': description,
                'genero': genero_final,
                'elenco': elenco,
                'direcao': direcao,
                'ano_raw': ano,
                'ano_lancamento': ano_int,
                'duracao': duracao,
                'temporadas': temporadas_float,
                'episodios': episodios,
                'data_extracao': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).strftime('%Y-%m-%d %H:%M:%S')),
                'ano_extracao': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).year),
                'mes_extracao': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).month),
                'dia_extracao': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).day)

            }
        
        return metadata
    
    async def get_series_metadata(self):
        '''
            Returns all the movie metadata using asyncio and aiohttp

            Returns:
                list of dictionaries with html content for every movie
        '''

        movies_data = await GetSeriesData(self._category).get_series_data()
        results = []
        async with aiohttp.ClientSession(headers = {'encoding':'utf-8'}) as session:
                tasks = [self.get_urls(session = session, url = x['url'], uuid = x['uuid'], series_name = x['name'], series_url = x['url'], logo = x['logo'], duracao = x['duracao']) for x in movies_data]
                html_pages = await asyncio.gather(*tasks)
                results.append(html_pages)

        # Salvando dados brutos
        # path = os.path.join(os.getcwd(), 'data', 'series') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'data', 'series')

        # pq.write_to_dataset(
        #     table = pa.Table.from_pandas(pd.DataFrame(results[0])),
        #     root_path = path,
        #     existing_data_behavior = 'delete_matching',
        #     basename_template = f"{datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).year}_series" + "{i}.parquet",
        #     use_legacy_dataset = False
        # )

        return results
    
    @st.cache_data(ttl = 86400)
    def get_urls_sync(self, url, duracao, uuid, series_name, series_url, logo):

        r = requests.get(url)
        soup = BeautifulSoup(r.text, 'html.parser')

        # DESCRIPTION
        try:
            description = soup.find('p',{'itemprop':'description'}).text
        except:
            description = 'Null'

        # METADADOS
        try:
            dados_gerais = {
                re.sub(r"\s*:*", "", x.find('span').text): x.find_all('a') 
                for x in soup.find('div', 'dec-review-meta').find_all('li')
            }

            dados_gerais_final = {
                k:', '.join([i.text for i in dados_gerais[k]]) 
                for k,_ in dados_gerais.items()
            }

            genero = dados_gerais_final['Gênero']
            elenco = dados_gerais_final['Elenco']
            direcao = dados_gerais_final['Direção']
            ano = dados_gerais_final['Ano']
        except:
            genero = 'Null'
            elenco = 'Null'
            direcao = 'Null'
            ano = 'Null'

        # GENERO AJUSTADO
        try:
            genero = re.sub(r'\s*', '', genero).split(',')
            genero.sort()
            genero_final = ', '.join(genero)
        except:
            genero_final = 'Null'

        # Ano
        try:
            ano_int = int(ano.strip().replace(r'\n', ''))
        except:
            ano_int = None

        # Temporadas atualizadas - float
        try:
            temporadas_float = float(re.sub(r'[^\d]', '', soup.find('div', 'rowp').find('h4').text))
        except:
            temporadas_float = None
        
        # Temporadas atualizadas - string
        try:
            temporadas_raw = soup.find('div', 'rowp').find('h4').text
        except:
            temporadas_raw = None

        # LISTA TEMPORADAS
        try:
            lista_temporadas = [i.text for i in soup.find_all('button', 'accordion')]
        except:
            lista_temporadas = None

        # LEGENDAS
        try:
            dados = []
            for i in soup.find_all('div', 'mvcast-item'):
                temporada = re.search(r'(\d*)x(\d*)', i.find('h4').text).groups()[0]
                episodio = re.search(r'(\d*)x(\d*)', i.find('h4').text).groups()[1]
                legendas = [l.text for l in i.find_all('a')]

                dados.append(
                    {
                        # 'uuid': str(uuid),
                        'temporada': temporada,
                        'episodio': episodio, 
                        'legendas': legendas
                    }
                )
        except:
            dados = []

        # Episodios
        episodios = len(dados)

        # LOGO FOLDER
        try:
            logo_folder = os.path.join(os.getcwd(), 'assets', f'{self._category}', f'{uuid}.png') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'assets', f'{self._category}', f'{uuid}.png')
            image = open_image(uuid = uuid, category = self._category)
        except:
            logo_folder = ''
            image = None

        # Base final
        metadata = {
            'uuid': str(uuid),
            'name': series_name, 
            'categoria': self._category,
            'url': series_url, 
            'url_src': f'<a href="{series_url}" target="_blank">{series_name}</a>',
            'logo': logo,
            'logo_src': f'<img src="{logo}" width="100" height="150">',
            'logo_folder': logo_folder,
            'image': image,
            'description': description,
            'genero': genero_final,
            'elenco': elenco,
            'direcao': direcao,
            'ano_raw': ano,
            'ano_lancamento': ano_int,
            'duracao': duracao,
            'temporadas': temporadas_float,
            'episodios': episodios,
            'data_extracao': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).strftime('%Y-%m-%d %H:%M:%S')),
            'ano_extracao': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).year),
            'mes_extracao': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).month),
            'dia_extracao': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).day)

        }
        
        return metadata
    
    @st.cache_data(ttl = 86400)
    def get_series_metadata_sync(self):

        series_data = GetSeriesData(self._category).get_series_data_sync()
        results = []
        
        with ThreadPoolExecutor(max_workers = 10) as executor:
            futures = {
                executor.submit(
                    self.get_urls_sync, 
                    url = x['url'], 
                    uuid = x['uuid'], 
                    series_name = x['name'], 
                    series_url = x['url'], 
                    logo = x['logo'], 
                    duracao = x['duracao']
                ) 
                for x in series_data
            }

            for future in futures:
                result = future.result() 
                results.append(result)

        return results