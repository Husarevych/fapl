import requests
import pandas as pd
from time import sleep
from datetime import datetime, timedelta, date
from bs4 import BeautifulSoup


class Scraper:
    
    def __init__(self, data, mode='incremental'):
        """
        Инициализация парсера с заданным режимом работы.
        :param mode: Режим парсинга ('full' или 'incremental').
        """
        self.data = data
        self.mode = mode
        self.url = 'http://fapl.ru/news/'

    def scraper(self):
        
        result = {}
        page = 0
        flag = False

        while True:
            while True:
                try:
                    responce = requests.get(self.url if not page else f"{self.url}?skip={page}")
                    break
                except requests.exceptions.RequestException as e:
                    print(f"Request failed: {e}")
                    sleep(2)
            responce.encoding = 'Windows-1251'
            soup = BeautifulSoup(responce.text, 'html.parser')
            news = soup.find_all('div', class_='block news')
            
            if not news:
                print("Нет новостей на странице. Завершение парсинга.")
                break
            
            for article in news:
                
                post = article.find('h3').find('a')['href'].strip()
                post_id = post.split('/')[2]
                url = f"http://fapl.ru{post}"

                post_comments = int(article.find('p', class_='f-r').text.split(' (')[1].replace(')', '').strip())
                
                while True:
                    try:
                        post_response = requests.get(url)
                        break
                    except Exception as e:
                        print(f"Request failed: {e}")
                        sleep(2)

                post_response.encoding = post_response.apparent_encoding
                post_soup = BeautifulSoup(post_response.text, 'html.parser')
                
                post_header = post_soup.find('div', class_='block').find('h2').text.strip()
                post_content = ' '.join(i.text.replace('\n\r\n', '').strip() for i in post_soup.find('div', class_='content').find_all('p')).strip()
                post_tags = post_soup.find('div', class_='info').find('p', class_='tags').text.strip()
                post_visits = post_soup.find('p', class_='visits f-l').text.strip().split(': ')[1]
                post_time = datetime.strptime(post_soup.find('p', class_='date f-r').text.strip(), '%d.%m.%Y %H:%M')
                
                if self.mode == 'incremental' and post_id in self.data:
                    flag = True
                    break
                elif self.mode == 'full' and post_time < datetime(2024, 1, 1):
                    flag = True
                    break
                
                wrap = {'header': post_header,
                        'content': post_content,
                        'time': post_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'post_visits': post_visits,
                        'post_comments': post_comments,
                        'post_tags': post_tags}
                
                result.setdefault(post_id, wrap)

            page += 20
            
            print(f"Переход на следующую страницу: {page} записей просмотрено.")
            
            if flag: break

        return result