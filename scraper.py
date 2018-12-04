# -*- coding: utf-8 -*-

import os
import sys
import requests
from bs4 import BeautifulSoup
from mongo_setup import Database
import gridfs
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
import logging
import re
import pymongo


# logging statuses
IN_PROCESS = "IN_PROCESS"
SUCCESS = "SUCCESS"
FAILED = "FAILED"

# path to project dirs
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
DRIVER_BIN = os.path.join(PROJECT_ROOT, "bin/chromedriver")

requests.packages.urllib3.disable_warnings()  # turn off SSL warnings


class NewsProvider:
    '''
    News resource class that encapsulates
    name of the web resource and it's URL
    '''

    def __init__(self, name, url):
        self.name = name
        self.url = url

    def __str__(self):
        return self.name


class Scraper:
    '''
    Scraper class that scrapes through resources like ukr.net and tsn.ua
    and retrieves news articles from them.
    Main functionality:
    - search by category word
    - search by text occurence in the news article

    Parsed articles and categories are written to MongoDB storage
    Retrieved images are downloaded to local storage
    Number of maximum parsed articles is set to attribute 'limit'
    '''

    # news resource URLs
    tsn_resource = NewsProvider('tsn.ua', 'https://tsn.ua/')
    ukrnet_resource = NewsProvider('ukr.net', 'https://www.ukr.net/')

    def __init__(self, limit=10):
        self.limit = limit                                                  # max number of articles per category
        self.db = Database('scraper_db').connect_db()                       # MongoDB connection by name
        self.category_coll = self.init_collection('categories')             # initialize MongoDB category collection
        self.articles_coll = self.init_collection('articles')               # initialize MongoDB articles collection
        self.logger = self.init_logger()                                    # initialize logging
        self.driver = self.init_webdriver()                                 # start Chrome webdriver
        self.image_storage = os.path.join(PROJECT_ROOT, "image_storage/")   # set path to local image storage

    def init_logger(self):
        '''
        Initialize log file.
        '''
        logger = logging.getLogger('scraper_app')
        logger.setLevel(logging.INFO)

        # create a file handler
        handler = logging.FileHandler('scraper_logfile.log')
        handler.setLevel(logging.INFO)

        # create a logging format
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        # add the handlers to the logger
        logger.addHandler(handler)
        return logger

    def init_webdriver(self):
        options = webdriver.ChromeOptions()
        options.add_argument('headless')
        return webdriver.Chrome(executable_path=DRIVER_BIN, chrome_options=options)

    def init_collection(self, name):
        '''
        сreates new MongoDB collection
        if already exists, remove old data from it
        '''
        if name in self.db.collection_names():
            self.db[name].drop()
        return self.db[name]

    def insert_one_to_collection(self, data, collection):
        '''
        inserts one data instance to specified MongoDB collection
        '''
        try:
            collection.insert_one(data)
            self.logger.info('%s - %s', IN_PROCESS, 'INSERTING DATA TO DB')
        except (pymongo.errors.PyMongoError, TypeError):
            self.logger.exception('%s - %s', FAILED, 'ERROR WHEN INSERTING DATA')

    def insert_many_to_collection(self, data, collection):
        '''
        inserts multiple data to specified MongoDB collection
        '''
        try:
            collection.insert_many(data)
            self.logger.info('%s - %s', IN_PROCESS, 'INSERTING MULTIPLE DATA TO DB')
        except (pymongo.errors.PyMongoError, TypeError):
            self.logger.exception('%s - %s', FAILED, 'ERROR WHEN INSERTING DATA')


    def download_image(self, image_url):
        '''
        download images by URL from news articles to local storage
        '''
        local_filename = image_url.split('/')[-1].split("?")[0]

        r = requests.get(image_url, stream=True, verify=False)
        with open(self.image_storage + local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                f.write(chunk)
        self.logger.info('%s - %s', IN_PROCESS, 'DOWNLOADING IMAGE')

    def upload_image_to_mongo(self, image_url):
        '''
        upload images by URL to MongoDB collection (not used)
        '''
        response = requests.get(image_url, stream=True)
        local_filename = image_url.split('/')[-1].split("?")[0]
        fs = gridfs.GridFS(self.db)
        img = response.raw.read()
        fs.put(img, filename=local_filename)

    def get_page_content(self, resource_name, url):
        '''
        invoke right method depending on the resource type (static/dynamic)
        '''
        if resource_name == 'tsn.ua':
            return self.get_request_content(url)
        else:
            return self.get_webdriver_content(url)

    def get_request_content(self, url):
        '''
        get page source by connecting to URL with requests
        '''
        try:
            r = requests.get(url)
            r.raise_for_status()
            return r.text
        except requests.exceptions.HTTPError as err:
            print(err)
            sys.exit(1)

    def get_webdriver_content(self, url):
        '''
        run Web driver to get page source with specific URL
        '''
        try:
            self.driver.get(url)
        except WebDriverException:
            self.driver = self.init_webdriver()
        page = self.driver.page_source
        return page

    def parse_page_content(self, resource_name, url):
        '''
        invoke Web driver to get page content by URL.
        pass page content to BeautifulSoup for parsing
        '''
        page_obj = self.get_page_content(resource_name, url)
        soup = BeautifulSoup(page_obj, "lxml")
        return soup

    def tsn_categories(self):
        '''
        parse tsn news articles' categories
        '''
        categories = self.gather_categories(self.tsn_resource.url, self.tsn_resource.name, 'ul.c-app-nav-more-list li a')
        self.logger.info('%s - %s', IN_PROCESS, 'PARSING TSN CATEGORIES')
        return categories

    def ukrnet_categories(self):
        '''
        parse ukr.net news articles' categories
        '''
        categories = self.gather_categories(self.ukrnet_resource.url, self.ukrnet_resource.name, 'h2.feed__section--title a')
        self.logger.info('%s - %s', IN_PROCESS, 'PARSING UKR.NET CATEGORIES')
        return categories

    def gather_categories(self, url, resource_name, selector):
        '''
        scrape each web resource to retrieve news categories
        for each category create object with category name and URL
        write this data to MongoDB categories collection
        return a list of categories objects
        '''
        categories = []
        soup = self.parse_page_content(resource_name, url)
        all_categories = soup.select(selector)

        for item in all_categories:
            category = {}
            link = str(item.attrs.get('href'))
            if link.startswith('javascript'):
                continue
            if not link.startswith('https:'):
                link = 'https:' + link
            category['link'] = link
            category['name'] = item.get_text().strip()
            categories.append(category)

        self.insert_many_to_collection(categories, self.category_coll)
        return categories

    def search_by_category(self, category_name):
        '''
        search for website resource that has specific category name
        retrieve news articles in this category
        '''
        category_name = category_name.decode('utf-8').lower()
        category_list = []
        category_list += self.tsn_categories()
        category_list += self.ukrnet_categories()

        # check if such category name exists
        try:
            category_obj = next(item for item in category_list if item['name'].lower() == category_name)
        except StopIteration:
            self.logger.exception('%s - %s', FAILED, 'STOP ITERATION ERROR')
            return False

        link = category_obj['link']
        if 'ukr.net' in link:
            articles = self.get_ukrnet_articles(category_name, link)
        else:
            articles = self.get_tsn_articles(category_name, link)
        self.logger.info('%s - %s', SUCCESS, 'SEARCH BY CATEGORY')
        return articles

    def get_ukrnet_articles(self, category_name, url):
        '''
        retrieve all articles from ukr.net by given category link
        write these articles to MongoDB articles collection
        '''
        count = 0
        result = []
        soup = self.parse_page_content(self.ukrnet_resource.name, url)
        all_articles = soup.select('div.im-tl a')
        for item in all_articles:
            if count <= self.limit:
                article = {}
                link = item.attrs.get('href')
                if link.startswith('//'):
                    link = link[2:]
                article['link'] = link
                article['category'] = category_name
                article['content'] = item.contents[0]
                result.append(article)
                self.insert_one_to_collection(article, self.articles_coll)
            else:
                break
            count += 1
        self.logger.info('%s - %s', IN_PROCESS, 'PARSING UKR.NET ARTICLES')
        return result

    def get_tsn_articles(self, category_name, url):
        '''
        retrieve all articles from tsn.ua by given category link
        write these articles to MongoDB articles collection
        '''
        count = 0
        result = []

        data = []  # temporary storage

        # first parse through the list of articles
        soup = self.parse_page_content(self.tsn_resource.name, url)
        all_articles = soup.select('div.c-entry-embed a.c-post-img-wrap')
        for item in all_articles:

            # retrieve limit amount of articles
            if count <= self.limit:
                article = {}
                link = item.attrs.get('href')
                img_src = item.find('img').get('src')
                if link.endswith(".html"):
                    article['link'] = link
                    if img_src:
                        if not img_src.startswith(("data:image", "javascript")):
                            article['img_src'] = img_src
                            self.download_image(img_src)

                    article['category'] = category_name
                    data.append(article)
                count += 1
            else:
                break

        # then iterate over each article
        for article in data:
            new_soup = self.parse_page_content(self.tsn_resource.name, article['link'])
            news_content = new_soup.select('div.e-content p')

            text_content = [] # article content
            for chunk in news_content:
                text_content.append(chunk.get_text().strip(''))
            article_text = ' '.join(text_content)

            news_header = new_soup.select('div.c-post-meta h1')  # article title
            if news_header:
                header_text = "".join(news_header[0].contents)

            article_image = new_soup.find('figure', class_='js-lightgallery')
            if article_image:
                if not article_image.startswith(("data:image", "javascript")):
                    img_src = article_image.find('img').get('src')  # articles image
                    self.download_image(img_src)

            news_chunk = {}
            news_chunk['category'] = article['category']
            news_chunk['link'] = article['link']
            news_chunk['title'] = header_text
            # news_chunk['title'] = ''
            news_chunk['content'] = article_text
            news_chunk['images'] = []
            if 'img_src' in article:
                news_chunk['images'].append(article['img_src'])  # caption image
            if article_image:
                news_chunk['images'].append(img_src)  # article image

            result.append(news_chunk)
            self.insert_one_to_collection(news_chunk, self.articles_coll)

        self.logger.info('%s - %s', IN_PROCESS, 'PARSING TSN ARTICLES')
        return result

    def search_by_text(self, text):
        '''
        search by specific text occurance in website resources
        '''
        category_links = []
        category_links += self.ukrnet_categories()
        category_links += self.tsn_categories()
        result = self.website_search_by_text(text, category_links)
        self.logger.info('%s - %s', SUCCESS, 'SEARCH BY TEXT')
        return result

    def website_search_by_text(self, text_searched, category_links):
        '''
        search news article by text in each of the passed category links
        '''
        result = []

        text_searched = text_searched.decode('utf-8')
        for link in category_links:
            # # check if enough number of articles is parsed
            # if len(result) == self.limit:
            #     break

            article = {}

            # define which web resource link belongs to
            if self.tsn_resource.name in link['link']:
                soup = self.parse_page_content(self.tsn_resource.name, link['link'])
            else:
                soup = self.parse_page_content(self.ukrnet_resource.name, link['link'])

            # in each category page retrieve articles that contain specific text
            all_articles = soup.find_all('a', text=re.compile(text_searched))
            for item in all_articles:
                article['link'] = item.attrs.get('href')
                article['category'] = link['name']
                article['content'] = (item.contents[0].strip())
                self.insert_one_to_collection(article, self.articles_coll)
                result.append(article)
        return result

    def collect_ukrnet_articles(self):
        '''
        collects ukr.net news articles in each category (outdated)
        '''
        categories = self.ukrnet_categories()

        for category in categories:
            count = 0
            soup = self.parse_page_content(self.ukrnet_resource.name, category['link'])

            all_articles = soup.select('div.im-tl a')
            for item in all_articles:
                if count < self.limit:
                    article = {}
                    link = item.attrs.get('href')
                    article['link'] = link
                    article['category'] = category['name']
                    article['content'] = item.contents[0].encode('utf-8')
                    self.insert_one_to_collection(article, self.articles_coll)
                else:
                    break
                count += 1

    def run(self):
        # self.tsn_categories()
        # self.ukrnet_categories()
        # self.get_ukrnet_articles('Головне', 'https://www.ukr.net/news/main.html')
        self.search_by_category('головне')
        # self.get_tsn_articles('Економіка', "https://tsn.ua/groshi")
        # self.search_by_text('авто')
        self.driver.quit()


if __name__ == '__main__':
    scraper = Scraper()
    scraper.run()

























