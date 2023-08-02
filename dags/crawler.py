from bs4 import BeautifulSoup
from datetime import date
import requests
import get_all_variables as gav



def extract_id_movie(url: str):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    div = soup.find("div",{"class":"a-box-inner"})
    title = div.find("a",{"class":"a-link-normal"}).attrs['href']
    id = title.split('/')[4]

    return id
                      
def crawl_box_office(date: date):
    url_prefix = gav.box_office_path

    url = f"{url_prefix}{date}/"

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


        fact_movie.append(movie_daily_info)

    # print(fact_movie)

    return fact_movie


def crawl_imdb(date: date):

    box_items = crawl_box_office(date)
    dim_movie = []
    url_prefix = gav.imdb_path
    user_agent = gav.user_agent
    
    for movie in box_items:
        dim_items = {}
        id = movie['id']
        url = f"{url_prefix}{id}/"

        headers = {'User-agent': user_agent}
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')

        dim_items['title'] = soup.find("span", {"class":"fDTGTb"}).text
        dim_items['movie_id'] = id
        dim_items['url'] = url
        dim_items['director'] = soup.find("a", 
                                          {"class": "ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link"}).text

        dim_movie.append(dim_items)

    

    # print(dim_movie)
    return dim_movie




#if __name__ == '__main__':
    #crawl_box_office(date=date(2023, 7, 27))
    #crawl_imdb(date=date(2023, 7, 27))


        