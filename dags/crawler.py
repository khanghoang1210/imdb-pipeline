from bs4 import BeautifulSoup
from datetime import date
import requests
#from config.box_office import BoxOffice


#from src.utils import get_variables as gav

def extract_id_movie(url: str):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    div = soup.find("div",{"class":"a-box-inner"})
    title = div.find("a",{"class":"a-link-normal"}).attrs['href']
    id = title.split('/')[4]

    return id
                      
def crawl_box_office(date: date):
    # url_prefix = gav.box_url

    # url = f"{url_prefix}{date}/"
    url = f"https://www.boxofficemojo.com/date/{date}"

    response = requests.get(url)

    soup = BeautifulSoup(response.content, 'html.parser')

    rows = soup.find_all("tr")
    fact_movie = []

    for row in rows[1:]:
        movie_daily_info = {}
        movie_daily_info['rank'] = row.find("td",{"class":"mojo-header-column"}).text

        revenue = row.find("td",{"class":"mojo-field-type-money"}).text
        movie_daily_info['revenue'] = revenue[1:]

        movie_daily_info['partition_date'] = date.strftime("%d-%m-%Y")

        href = row.find("td",{"class": "mojo-field-type-release"}).find("a").attrs['href']
        url_detail = url[:29] + href
        movie_daily_info['id'] = extract_id_movie(url_detail)

        # box_office_instance = BoxOffice(
        #     id=movie_daily_info['id'],
        #     rank=movie_daily_info['rank'],
        #     revenue=movie_daily_info['revenue'],
        #     partition_date=movie_daily_info['partition_date']
        # )
        # fact_movie.append(box_office_instance.to_dict())


        fact_movie.append(movie_daily_info)

    # print(fact_movie)
    # print(len(fact_movie))
    # for mv in fact_movie:
    #     print(mv['id'])
    return fact_movie


def crawl_imdb(id):

    dim_movie = []
    #id = context['task_instance'].xcom_pull(task_ids='exrtract_id_movie')
    url = f'https://www.imdb.com/title/{id}/'
    headers = {'User-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    title = soup.find("span", {"class":"fDTGTb"}).text
    movie_id = id
    url = url
    director = soup.find("a", {"class": "ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link"}).text

    dim_movie.append(movie_id)
    dim_movie.append(title)
    dim_movie.append(director)
    dim_movie.append(url) 
    

    return dim_movie




if __name__ == '__main__':
    crawl_box_office(date=date(2023, 7, 27))
    #crawl_imdb()


        