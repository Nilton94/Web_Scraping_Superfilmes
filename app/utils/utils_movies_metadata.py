import aiohttp
import asyncio
import re
import requests
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from dataclasses import dataclass
from utils.utils_uuid import generate_uuid
from utils.utils_pages import GetPageNumber
from utils.utils_movies import GetMovieData
from utils.utils_images import open_image
import datetime, pytz
from PIL import Image
import os
import streamlit as st

@dataclass
class GetMovieMetadata:
    _category: str = 'filmes'

    async def get_urls(self, session, url, uuid, movie_name, movie_url, logo):
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

            # NOTA
            try:
                nota = soup.find('div', 'dec-review-dec').find('div', 'ratting').find('a').text
            except:
                nota = 'Null'

            # NOTA FLOAT
            try:
                nota_float = float(re.match(r'Nota:\s(\d.*)/', nota).group(1))
            except:
                nota_float = None

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
                duracao = dados_gerais_final['Duração']
            except:
                genero = 'Null'
                elenco = 'Null'
                direcao = 'Null'
                ano = 'Null'
                duracao = 'Null'

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

            # Duração
            duracao_treated = re.sub(r'\n|\s|[^\dhmDH]', '', duracao)
            duracao_lista = [
                {
                    'raw': duracao_treated,
                    'h': i[0] if duracao_treated.__contains__('h') else 0,
                    'm': i[1] if duracao_treated.__contains__('h') else i[0]

                }
                for i in re.findall(r'(\d*)h?\s*(\d*)m?', duracao_treated)
                if i[0] != '' or i[1] != ''
            ]
            try:
                # horas = float(re.search(r'(\d*)(h|)((\d*)|)(m|)', duracao_treated.strip()).group(1))
                horas = float(duracao_lista[0]['h'])
            except:
                horas = None

            try:
                # minutos = float(re.search(r'(\d*)(h|)((\d*)|)(m|)', duracao_treated.strip()).group(2))
                minutos = float(duracao_lista[0]['m'])
            except:
                minutos = None

            try:
                duracao_total = float(horas)*60 + float(minutos)
            except:
                duracao_total = None

            # LEGENDAS
            try:
                legendas = set([re.match(r'.*(leg|dub).*',i.find('img')['src']).group(1) for i in soup.find_all('div', 'vd-it')])
            except:
                legendas = 'Null'

            # LOGO FOLDER
            try:
                logo_folder = os.path.join(os.getcwd(), 'assets', f'{self._category}', f'{uuid}.png') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'assets', f'{self._category}', f'{uuid}.png')
                image = open_image(uuid = uuid, category = self._category)
            except:
                logo_folder = ''
                image = None
            
            # BASE
            metadata = {
                'uuid': str(uuid),
                'name': movie_name, 
                'categoria': self._category,
                'url': movie_url, 
                'url_src': f'<a href="{movie_url}" target="_blank">{movie_name}</a>',
                'logo': logo,
                'logo_src': f'<img src="{logo}" width="100" height="150">',
                'logo_folder': logo_folder,
                'image': image,
                'nota': nota,
                'nota_float': nota_float,
                'description': description,
                'genero': genero_final,
                'elenco': elenco,
                'direcao': direcao,
                'ano_raw': ano,
                'ano_lancamento': ano_int,
                'duracao': duracao_treated,
                'duracao_raw': duracao,
                'horas': horas,
                'minutos': minutos,
                'duraca_total_minutos': duracao_total,
                'legendas': ', '.join(legendas),
                'data_extracao': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).strftime('%Y-%m-%d %H:%M:%S')),
                'ano_extracao': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).year),
                'mes_extracao': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).month),
                'dia_extracao': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).day)

            }
        
        return metadata
    
    async def get_movie_metadata(self):
        '''
            Returns all the movie metadata using asyncio and aiohttp

            Returns:
                list of dictionaries with html content for every movie
        '''

        try:
            movies_data = await GetMovieData(self._category).get_movie_data()
            results = []
            
            async with aiohttp.ClientSession(headers = {'encoding':'utf-8'}) as session:
                    tasks = [
                        self.get_urls(session = session, url = x['movie_url'], uuid = x['uuid'], movie_name = x['movie_name'], movie_url = x['movie_url'], logo = x['logo'])
                        for x in movies_data
                    ]
                    html_pages = await asyncio.gather(*tasks)
                    results.append(html_pages)
                    
        except Exception as e:
            return f'Erro: {e}'

        # Salvando dados brutos
        # path = os.path.join(os.getcwd(), 'data', 'filmes') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'data', 'filmes')

        # pq.write_to_dataset(
        #     table = pa.Table.from_pandas(pd.DataFrame(results[0])),
        #     root_path = path,
        #     existing_data_behavior = 'delete_matching',
        #     basename_template = f"{datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).year}_filmes" + "{i}.parquet",
        #     use_legacy_dataset = False
        # )
        
        return results
    
    @st.cache_data(ttl = 86400)
    def get_urls_sync(self, url, uuid, movie_name, movie_url, logo):

        r = requests.get(url)
        soup = BeautifulSoup(r.text, 'html.parser')

        # NOTA
        try:
            nota = soup.find('div', 'dec-review-dec').find('div', 'ratting').find('a').text
        except:
            nota = 'Null'

        # NOTA FLOAT
        try:
            nota_float = float(re.match(r'Nota:\s(\d.*)/', nota).group(1))
        except:
            nota_float = None

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
            duracao = dados_gerais_final['Duração']
        except:
            genero = 'Null'
            elenco = 'Null'
            direcao = 'Null'
            ano = 'Null'
            duracao = 'Null'

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

        # Duração
        duracao_treated = re.sub(r'\n|\s|[^\dhmDH]', '', duracao)
        duracao_lista = [
            {
                'raw': duracao_treated,
                'h': i[0] if duracao_treated.__contains__('h') else 0,
                'm': i[1] if duracao_treated.__contains__('h') else i[0]

            }
            for i in re.findall(r'(\d*)h?\s*(\d*)m?', duracao_treated)
            if i[0] != '' or i[1] != ''
        ]
        try:
            # horas = float(re.search(r'(\d*)(h|)((\d*)|)(m|)', duracao_treated.strip()).group(1))
            horas = float(duracao_lista[0]['h'])
        except:
            horas = None

        try:
            # minutos = float(re.search(r'(\d*)(h|)((\d*)|)(m|)', duracao_treated.strip()).group(2))
            minutos = float(duracao_lista[0]['m'])
        except:
            minutos = None

        try:
            duracao_total = float(horas)*60 + float(minutos)
        except:
            duracao_total = None

        # LEGENDAS
        try:
            legendas = set([re.match(r'.*(leg|dub).*',i.find('img')['src']).group(1) for i in soup.find_all('div', 'vd-it')])
        except:
            legendas = 'Null'

        # LOGO FOLDER
        try:
            logo_folder = os.path.join(os.getcwd(), 'assets', f'{self._category}', f'{uuid}.png') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'assets', f'{self._category}', f'{uuid}.png')
            image = open_image(uuid = uuid, category = self._category)
        except:
            logo_folder = ''
            image = None
        
        # BASE
        metadata = {
            'uuid': str(uuid),
            'name': movie_name, 
            'categoria': self._category,
            'url': movie_url, 
            'url_src': f'<a href="{movie_url}" target="_blank">{movie_name}</a>',
            'logo': logo,
            'logo_src': f'<img src="{logo}" width="100" height="150">',
            'logo_folder': logo_folder,
            'image': image,
            'nota': nota,
            'nota_float': nota_float,
            'description': description,
            'genero': genero_final,
            'elenco': elenco,
            'direcao': direcao,
            'ano_raw': ano,
            'ano_lancamento': ano_int,
            'duracao': duracao_treated,
            'duracao_raw': duracao,
            'horas': horas,
            'minutos': minutos,
            'duraca_total_minutos': duracao_total,
            'legendas': ', '.join(legendas),
            'data_extracao': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).strftime('%Y-%m-%d %H:%M:%S')),
            'ano_extracao': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).year),
            'mes_extracao': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).month),
            'dia_extracao': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).day)

        }
        
        return metadata
    
    @st.cache_data(ttl = 86400)
    def get_movie_metadata_sync(self):

        movies_data = GetMovieData(self._category).get_movie_data_sync()
        results = []
        
        with ThreadPoolExecutor(max_workers = 10) as executor:
            futures = {
                executor.submit(
                    self.get_urls_sync, 
                    url = x['movie_url'], 
                    uuid = x['uuid'], 
                    movie_name = x['movie_name'], 
                    movie_url = x['movie_url'], 
                    logo = x['logo']
                ) 
                for x in movies_data
            }

            for future in futures:
                result = future.result() 
                results.append(result)

        return results