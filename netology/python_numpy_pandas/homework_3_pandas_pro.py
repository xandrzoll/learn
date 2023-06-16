import pandas as pd

from datetime import datetime

# task 1 Для датафрейма log из материалов занятия создайте столбец source_type по правилам
data_log = pd.read_csv('data/visit_log.csv', sep=';')

def check_source_type(log: pd.Series) -> str:
    if log['traffic_source'] in ('yandex', 'google'):
        return 'organic'
    if log['traffic_source'] in ('paid', 'email'):
        if log['region'] == 'Russia':
            return 'ad'
        else:
            return 'other'
    return log['traffic_source']


data_log['source_type'] = data_log.apply(check_source_type, axis=1)
data_log.head()
#     timestamp    visit_id  ... traffic_source source_type
# 0  1549980692  e3b0c44298  ...         yandex     organic
# 1  1549980704  6e340b9cff  ...         direct      direct
# 2  1549980715  96a296d224  ...         yandex     organic
# 3  1549980725  709e80c884  ...         yandex     organic
# 4  1549980736  df3f619804  ...         yandex     organic
# ==========================

# task 2 В файле URLs.txt содержатся URL страниц новостного сайта.
# Вам нужно отфильтровать его по адресам страниц с текстами новостей

data_urls = pd.read_csv('data/URLs.txt')
data_urls_news = data_urls[data_urls['url'].str.contains(r'\/\d{8}-.+')]
data_urls_news.head()
#                                                 url
# 3  /politics/36188461-s-marta-zhizn-rossiyan-susc...
# 4  /world/36007585-tramp-pridumal-kak-reshit-ukra...
# 5  /science/36157853-nasa-sobiraet-ekstrennuyu-pr...
# 6  /video/36001498-poyavilis-pervye-podrobnosti-g...
# 7  /world/36007585-tramp-pridumal-kak-reshit-ukra...
# =============================

#  task 3 Посчитайте среднее время жизни пользователей, которые выставили более 100 оценок

data_ratings = pd.read_csv('data/ratings.csv')
data_ratings['datetime'] = pd.to_datetime(data_ratings['timestamp'], unit='s')
data_ratings_users_100 = data_ratings.groupby('userId', as_index=False).agg({'datetime': ['count', 'max', 'min']})
data_ratings_users_100.columns = ['user_id', 'count', 'dt_max', 'dt_min']
data_ratings_users_100['dt_delta'] = data_ratings_users_100['dt_max'] - data_ratings_users_100['dt_min']
data_ratings_users_100 = data_ratings_users_100[data_ratings_users_100['count'] > 100]
print(data_ratings_users_100['dt_delta'].mean())
# 463 days 21:28:27.449612408
# ===========================

# task 4
rzd = pd.DataFrame(
    {
        'client_id': [111, 112, 113, 114, 115],
        'rzd_revenue': [1093, 2810, 10283, 5774, 981]
    }
)
auto = pd.DataFrame(
    {
        'client_id': [113, 114, 115, 116, 117],
        'auto_revenue': [57483, 83, 912, 4834, 98]
    }
)
air = pd.DataFrame(
    {
        'client_id': [115, 116, 117, 118],
        'air_revenue': [81, 4, 13, 173]
    }
)
client_base = pd.DataFrame(
    {
        'client_id': [111, 112, 113, 114, 115, 116, 117, 118],
        'address': ['Комсомольская 4', 'Энтузиастов 8а', 'Левобережная 1а', 'Мира 14', 'ЗЖБИиДК 1',
                    'Строителей 18', 'Панфиловская 33', 'Мастеркова 4']
    }
)
merged_df = pd.merge(rzd, auto, on='client_id', how='outer')
merged_df = pd.merge(merged_df, air, on='client_id', how='outer')
merged_df.fillna(0, inplace=True)
print(merged_df)
#    client_id  rzd_revenue  auto_revenue  air_revenue
# 0        111       1093.0           0.0          0.0
# 1        112       2810.0           0.0          0.0
# 2        113      10283.0       57483.0          0.0
# 3        114       5774.0          83.0          0.0
# 4        115        981.0         912.0         81.0
# 5        116          0.0        4834.0          4.0
# 6        117          0.0          98.0         13.0
# 7        118          0.0           0.0        173.0

merged_df = pd.merge(merged_df, client_base, on='client_id', how='left')
print(merged_df)
#    client_id  rzd_revenue  auto_revenue  air_revenue          address
# 0        111       1093.0           0.0          0.0  Комсомольская 4
# 1        112       2810.0           0.0          0.0   Энтузиастов 8а
# 2        113      10283.0       57483.0          0.0  Левобережная 1а
# 3        114       5774.0          83.0          0.0          Мира 14
# 4        115        981.0         912.0         81.0        ЗЖБИиДК 1
# 5        116          0.0        4834.0          4.0    Строителей 18
# 6        117          0.0          98.0         13.0  Панфиловская 33
# 7        118          0.0           0.0        173.0     Мастеркова 4
