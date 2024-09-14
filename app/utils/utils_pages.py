import requests
from bs4 import BeautifulSoup
from dataclasses import dataclass
import re

@dataclass
class GetPageNumber:
    
    _category: str = 'filmes'
    
    def get_domain(self):
        '''
            Get the new domain
        '''

        try:
            link = 'https://www.google.com/search?q=superfilmes&oq=superfilmes&sourceid=chrome&ie=UTF-8'
            ret = requests.get(link)
            url = [i.text for i in BeautifulSoup(ret.text, 'html.parser').find_all('div') if re.match('superfilmes.*', i.text) != None][0]

            return url
        except:
            return 'superfilmes.red'
    
    def get_page_number(self):
        '''
            Extract the last page number of a given category
        '''
        
        url = f'https://{self.get_domain()}/{self._category}/1/'

        try:
            r = requests.get(
                url = url,
                headers = {'encoding':'utf-8'}
            )

            max_page = max(
                [
                    int(i.text) 
                    for i in BeautifulSoup(r.text, 'html.parser').find('ul','pagination').find_all('a') 
                    if i.text != ''
                ]
            )

            return max_page
            
        except Exception as e:
            return f'Erro: {e}'
        