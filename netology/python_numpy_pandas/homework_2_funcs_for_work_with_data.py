import pandas as pd


# task 1: Напишите функцию, которая классифицирует фильмы
def get_data() -> pd.DataFrame:
    data_movies = pd.read_csv(
        'data/movies.dat', sep='::', encoding='ISO-8859-1',
        engine='python', header=None, names=['movie_id', 'title', 'genres'], index_col='movie_id')
    data_ratings = pd.read_csv('data/ratings.dat', sep='::', encoding='ISO-8859-1',
        engine='python', header=None, names=['user_id', 'movie_id', 'rating', 'timestamp'])

    data_movies_rating = (
        data_movies.merge(
            data_ratings.groupby('movie_id')['rating'].mean(),
            how='left',
            on='movie_id'
        )
    )
    return data_movies_rating

def rating_class(rating: float) -> str:
    if rating <= 2:
        return 'низкий рейтинг'
    if rating <= 4:
        return 'средний рейтинг'
    return 'высокий рейтинг'


data = get_data()
data['class'] = data['rating'].apply(rating_class)
print(data.head())
#                                        title  ...            class
# movie_id                                      ...
# 1                           Toy Story (1995)  ...  высокий рейтинг
# 2                             Jumanji (1995)  ...  средний рейтинг
# 3                    Grumpier Old Men (1995)  ...  средний рейтинг
# 4                   Waiting to Exhale (1995)  ...  средний рейтинг
# 5         Father of the Bride Part II (1995)  ...  средний рейтинг


# task 2: Нужно написать гео-классификатор, который каждой строке сможет выставить географическую
#         принадлежность определённому региону

geo_data = {
    'Центр': ['москва', 'тула', 'ярославль'],
    'Северо-Запад': ['петербург', 'псков', 'мурманск'],
    'Дальний Восток': ['владивосток', 'сахалин', 'хабаровск']
}

data_keywords = pd.read_csv('netology/python_numpy_pandas/data/keywords.csv')

def get_region(keyword: str, geo_data: dict) -> str:
    keyword = keyword.lower()
    for key, items in geo_data.items():
        for item in items:
            if item.lower() in keyword:
                return key
    return 'undefined'


data_keywords['region'] = data_keywords['keyword'].apply(lambda x: get_region(x, geo_data))
print(data_keywords.head())
#          keyword     shows     region
# 0             вк  64292779  undefined
# 1  одноклассники  63810309  undefined
# 2          порно  41747114  undefined
# 3           ютуб  39995567  undefined
# 4      вконтакте  21014195  undefined
# .............
# 127 авито москва  979292    Центр
# 370 авито ру санкт петербург  425134    Северо-Запад

# task 3
import re


years = [_ for _ in range(1950, 2011)]
pattern_year = re.compile(r'\((\d{4})\)')
def production_year(title: str) -> int:
    year = pattern_year.findall(title)
    if not year:
        return 1900
    year = int(year[0])
    if year not in years:
        return 1900
    return year


data = get_data()
data['year'] = data['title'].apply(production_year)
data_years_rating = data.groupby('year', as_index=False)['rating'].mean().sort_values('rating', ascending=False)
print(data_years_rating.head())
#     year    rating
# 13  1962  3.803059
# 2   1951  3.795416
# 1   1950  3.758235
# 5   1954  3.748782
# 17  1966  3.731684
