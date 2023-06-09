import pandas as pd


data_movies = pd.read_csv(
    'data/movies.dat', sep='::', encoding='ISO-8859-1',
    engine='python', header=None, names=['movie_id', 'title', 'genres'], index_col='movie_id')
data_ratings = pd.read_csv('data/ratings.dat', sep='::', encoding='ISO-8859-1',
    engine='python', header=None, names=['user_id', 'movie_id', 'rating', 'timestamp'])

rating_5_gr = (
    data_ratings[data_ratings['rating'] == 5]
    .groupby('movie_id')['user_id'].count()

)
data_movies_rating = (
    data_movies.merge(
        rating_5_gr,
        how='left',
        on='movie_id'
    )
    .rename(columns={'user_id': 'rating_count'})
    .sort_values('rating_count', ascending=False)
    .head(5)
)
print(data_movies_rating)
#                                                       title  ... rating_count
# movie_id                                                     ...
# 2858                                 American Beauty (1999)  ...       1963.0
# 260               Star Wars: Episode IV - A New Hope (1977)  ...       1826.0
# 1198                         Raiders of the Lost Ark (1981)  ...       1500.0
# 1196      Star Wars: Episode V - The Empire Strikes Back...  ...       1483.0
# 858                                   Godfather, The (1972)  ...       1475.0

data_power = pd.read_csv('data/power.csv')
result = data_power[
    (data_power['country'].isin(['Estonia', 'Latvia', 'Lithuania']))
    & (data_power['year'].between(2005, 2010))
    & (data_power['category'].isin([4, 12, 21]))
    & (data_power['quantity'] > 0)
]['quantity'].sum()
print(f'{result=}')
# result=240580.0

data_html = pd.read_html('https://pythonworld.ru/tipy-dannyx-v-python/stroki-funkcii-i-metody-strok.html', encoding='utf-8')
print(data_html[0].head())
#                                 Функция или метод                                         Назначение
# 0  S = 'str'; S = "str"; S = '''str'''; S = """st...                                     Литералы строк
# 1                                 S = "s\np\ta\nbbb"                  Экранированные последовательности
# 2                                 S = r"C:\temp\new"  Неформатированные строки (подавляют экранирова...
# 3                                        S = b"byte"                                      Строка байтов
# 4                                            S1 + S2                      Конкатенация (сложение строк)
