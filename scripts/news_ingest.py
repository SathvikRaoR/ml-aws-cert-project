import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.s3_etl import *
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
from scripts.s3_etl import *

load_dotenv()
news_api_key = os.getenv('NEWS_API_KEY')
url = f"https://newsapi.org/v2/everything?q=finance OR stock OR economy&language=en&sortBy=publishedAt&apiKey={news_api_key}"
resp = requests.get(url)
articles = resp.json().get("articles", [])
df = pd.DataFrame(articles)
filename = f"data/news_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
df.to_csv(filename, index=False)
upload_file(filename, f"raw/news/{os.path.basename(filename)}")