import requests
import pandas as pd
from time import sleep
from datetime import datetime
from bs4 import BeautifulSoup


class Scraper:

    """ A class to handle web scraping tasks."""
    
    def __init__(self, data, mode='incremental'): 
        
        """ Initializes the scraper with a specified mode. 
        Args: 
            data (list): A list of post_id that already exist in the database. 
            mode (str): Mode of scraping ('full' or 'incremental'). Defaults to 'incremental'. """ 
        
        self.data = data 
        self.mode = mode 
        self.url = 'http://fapl.ru/news/' # Base URL for scraping news

    def scraper(self):
        
        """ Scrapes news articles from the specified URL and returns a dictionary with article details. 
        Returns: 
            dict: A dictionary where keys are post_ids and values are dictionaries of article details. """
        
        result = {}
        page = 0
        flag = False

        while True:
            while True:
                try:
                    responce = requests.get(self.url if not page else f"{self.url}?skip={page}") # Send request to URL
                    break
                except requests.exceptions.RequestException as e:
                    print(f"Request failed: {e}") # Print error message if request fails
                    sleep(2) # Wait for 2 seconds before retrying
            responce.encoding = 'Windows-1251' # Set the encoding for the response
            soup = BeautifulSoup(responce.text, 'html.parser') # Parse the HTML content
            news = soup.find_all('div', class_='block news') # Find all news blocks
            
            if not news:
                print("No news on the page. Ending scraping.") # Print message if no news is found
                break
            
            for article in news:
                
                post = article.find('h3').find('a')['href'].strip() # Extract post URL
                post_id = post.split('/')[2] # Extract post_id from URL
                url = f"http://fapl.ru{post}" # Complete URL for the post

                # Extract number of comments
                post_comments = int(article.find('p', class_='f-r').text.split(' (')[1].replace(')', '').strip())
                
                while True:
                    try:
                        post_response = requests.get(url) # Send request to post URL
                        break
                    except Exception as e:
                        print(f"Request failed: {e}") # Print error message if request fails
                        sleep(2) # Wait for 2 seconds before retrying

                post_response.encoding = post_response.apparent_encoding # Set the encoding for the response
                post_soup = BeautifulSoup(post_response.text, 'html.parser') # Parse the HTML content
                
                post_header = post_soup.find('div', class_='block').find('h2').text.strip() # Extract post header
                post_content = ' '.join(i.text.replace('\n\r\n', '').strip() for i in post_soup.find('div', class_='content').find_all('p')).strip() # Extract post content
                post_tags = post_soup.find('div', class_='info').find('p', class_='tags').text.strip() # Extract post tags
                post_visits = post_soup.find('p', class_='visits f-l').text.strip().split(': ')[1] # Extract post visits
                post_time = datetime.strptime(post_soup.find('p', class_='date f-r').text.strip(), '%d.%m.%Y %H:%M') # Extract post time
                
                if self.mode == 'incremental' and post_id in self.data:
                    flag = True
                    break # Exit the loop if in incremental mode and the post_id already exists
                elif self.mode == 'full' and post_time < datetime(2024, 1, 1):
                    flag = True
                    break # Exit the loop if in full mode and the post time is before 2024
                
                wrap = {'header': post_header,
                        'content': post_content,
                        'time': post_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'post_visits': post_visits,
                        'post_comments': post_comments,
                        'post_tags': post_tags}
                
                result.setdefault(post_id, wrap) # Add the post details to the result dictionary

            page += 20
            
            print(f"Moving to the next page: {page} records viewed.") # Print message indicating the next page
            
            if flag: break

        return result