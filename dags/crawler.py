from bs4 import BeautifulSoup
from datetime import datetime, date
import requests

#from src.utils import get_variables as gav

def exrtract_id_movie(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    div = soup.find("div",{"class":"a-box-inner"})
    title = div.find("a",{"class":"a-link-normal"}).attrs['href']
    id = title.split('/')[4]

    return id
                      
def crawl_box_office(date:date):
    # url_prefix = gav.box_url

    # url = f"{url_prefix}{date}/"
    url = f"https://www.boxofficemojo.com/date/{date}"

    response = requests.get(url)

    soup = BeautifulSoup(response.content, 'html.parser')

    rows = soup.find_all("tr")
    fact_movie = []

    for row in rows[1:]:
        movie_info = {}
        movie_info['rank'] = row.find("td",{"class":"mojo-header-column"}).text

        revenue = row.find("td",{"class":"mojo-field-type-money"}).text
        movie_info['revenue'] = revenue[1:]

        movie_info['partition_date'] = date.strftime("%d-%m-%Y")

        href = row.find("td",{"class": "mojo-field-type-release"}).find("a").attrs['href']
        url_detail = url[:29] + href
        movie_info['id'] = exrtract_id_movie(url_detail)

        fact_movie.append(movie_info)

    print(fact_movie)
    print(len(fact_movie))
    return fact_movie



if __name__ == '__main__':
    crawl_box_office(date=date(2023, 7, 27))


        