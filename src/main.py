import pandas as pd
import requests
from datetime import date, timedelta
import snowflake_connect as sfc

def runner():
    today = date.today()
    yesterday = today - timedelta(days=1)
    api_key = 'd7ae470895dd43c7a3dc49fc6ecb80a7'
    query='apple'

    base_url = f"https://newsapi.org/v2/everything?q={query}&from={yesterday}&to={today}&sortBy=publishedAt&apiKey={api_key}"
    print(base_url)

    response = requests.get(base_url)
    if response.status_code == 200:
        data = response.json()
        articles = data.get('articles', [])

        df = pd.DataFrame(articles)
        if not df.empty:
            df = df[['title', 'publishedAt', 'url', 'content', 'source', 'author', 'urlToImage']]
            df.columns = ['newsTitle', 'timestamp', 'url_source', 'content', 'source', 'author', 'urlToImage']
            print(df.head())  # Show the first few rows of the dataframe
        else:
            print("No articles found.")
        df.to_csv("news.csv", encoding='utf-8', index=False)

    else:
        print(f"Failed to fetch data: {response.status_code}")

if __name__ == "__main__":
    runner()
    sfc.connect_snowflake()