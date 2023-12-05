import requests
import pymongo
import schedule
import time
import logging
from pymongo import MongoClient
from bs4 import BeautifulSoup

# Настройка логгера
logging.basicConfig(filename='parser.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

client = MongoClient('mongodb://localhost:27017/')
db = client['BaseBenzinga']
collection = db['ParsData']

def create_database():
    # Создание базы данных и коллекции, если их нет
    dblist = client.list_database_names()
    if "BaseBenzinga" not in dblist:
        logging.info("Создание базы данных и коллекции")
        mydb = client["BaseBenzinga"]
        mycol = mydb["ParsData"]

def parse_article(url):
    response = requests.get(url)
    article_content = response.content
    article_soup = BeautifulSoup(article_content, 'html.parser')

    article_title = article_soup.select_one('h1.layout-title').text.strip()
    logging.info(f"Заголовок статьи: {article_title}")

    article_text_elements = article_soup.select('.layout-main [class^="sc-"] p, .layout-main [class^="sc-"] h4')
    if article_text_elements:
        article_text = '\n'.join(element.get_text(separator='\n', strip=True) for element in article_text_elements)
        logging.info(f"Текст статьи:\n{article_text}")
    else:
        logging.warning("Текст статьи не найден.")

    article_date = article_soup.select_one('.article-date-wrap').text.strip()
    logging.info(f"Дата и время публикации: {article_date}")

    article_data = {
        'title': article_title,
        'url': url,
        'text': article_text,
        'publication_date': article_date,
    }
    collection.insert_one(article_data)
    logging.info("Данные сохранены в БД.")

def main():
    create_database()

    url = "https://www.benzinga.com/news/"

    response = requests.get(url)
    html_content = response.content

    soup = BeautifulSoup(html_content, 'html.parser')

    news_items = soup.select('.content-feed-list .newsfeed-card')

    for item in news_items:
        title = item.select_one('.post-card-title').text.strip()
        news_link = item['href']

        logging.info(f"Ссылка: {news_link}")

        existing_news = collection.find_one({'url': news_link})
        if not existing_news:
            parse_article(news_link)
        else:
            logging.info("Эта новость уже в БД.")

if __name__ == "__main__":
    main()

    # Запуск задачи для периодического обновления данных
    schedule.every(40).seconds.do(main)

    while True:
        schedule.run_pending()
        time.sleep(1)