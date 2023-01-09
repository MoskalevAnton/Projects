#!/usr/bin/env python
# coding: utf-8

# <div class="alert alert-info" style="background:#ffdbf1;color:#2e00ab;border-left:7px solid #2e00ab">
# <b>Комментарий студента №0</b>
#     
# Привет, есть пара вопросов,почти не по проекту, все они ниже, приятной проверки.
# 
# </div>

# # Задача - проанализировать базу данных

# # Вывод - база данных работает исправно, запросы выполняются, связи между таблицами настроены корректно. 

# # Декомпозиция
# Задача
# Вывод
# 1 Шаг. Подключение к БД и предобработка данных
#     
#     * Подключение к БД
#     * Проверка на пропуски
#     * Проверка на дубликаты
#     * Просмотр таблиц
#     * Проверка названий столбцов
#     * Вывод по шагу
#     
#     
# 2 Шаг. Проверка работоспособности БД
# 
#     Проверка на выполнение различных запросов
#     
#     2.1 Количество книг, которые вышли после 1 января 2000 года;
#     2.2 Количество обзоров и средняя оценка для каждой книги;
#     2.3 Издательство, которое выпустило наибольшее число книг толще 50 страниц;
#     2.4 Автор с самой высокой средней оценкой книг(учитывая только книги с 50 и более оценками);
#     2.5 Среднее количество обзоров от пользователей, которые поставили больше 50 оценок. 
#     
# 3 Шаг. Вывод
#     

# <a id="1"></a> 
# ## Шаг. Подключение к БД и предобработка данных

# In[ ]:


# импортируем библиотеки
import pandas as pd
from sqlalchemy import create_engine


# In[ ]:


# устанавливаем параметры
db_config = {'user': 'praktikum_student', # имя пользователя
'pwd': 'Sdf4$2;d-d30pp', # пароль
'host': 'rc1b-wcoijxj3yxfsf3fs.mdb.yandexcloud.net',
'port': 6432, # порт подключения
'db': 'data-analyst-final-project-db'} # название базы данных
connection_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_config['user'],
 db_config['pwd'],
 db_config['host'],
 db_config['port'],
 db_config['db'])
# сохраняем коннектор
engine = create_engine(connection_string, connect_args={'sslmode':'require'}) 


# In[ ]:


#просматривать таблицы будем через цикл
def review(tabl):
    query = 'SELECT * FROM ' + tabl
    df = pd.io.sql.read_sql(query, con=engine)
    print(f'Пропусков: {df.isna().sum().sum()}')
    print(f'Дубликатов: {df.duplicated().sum()}')
    display(df.head())


# In[ ]:


review('books')


# Таблица books содержит данные о книгах:
# * book_id — идентификатор книги;
# * author_id — идентификатор автора;
# * title — название книги;
# * num_pages — количество страниц;
# * publication_date — дата публикации книги;
# * publisher_id — идентификатор издателя.

# In[ ]:


review('authors')


# Таблица authors cодержит данные об авторах:
# * author_id — идентификатор автора;
# * author — имя автора.

# In[ ]:


review('publishers')


# Таблица publishers содержит данные об издательствах:
# * publisher_id — идентификатор издательства;
# * publisher — название издательства;

# In[ ]:


review('ratings')


# Таблица ratings содержит данные о пользовательских оценках книг:
# * rating_id — идентификатор оценки;
# * book_id — идентификатор книги;
# * username — имя пользователя, оставившего оценку;
# * rating — оценка книги.

# In[ ]:


review('reviews')


# Таблица reviews содержит данные о пользовательских обзорах:
# * review_id — идентификатор обзора;
# * book_id — идентификатор книги;
# * username — имя автора обзора;
# * text — текст обзора.

# <a id="11"></a> 
# ### Вывод по 1 шагу
# 
# В ходе выполнения первого шага были проделаны следующие действия:
# 
#     * Подключение к базе данных
#     * проверка на пропуски (отсутствуют)
#     * проверка на дубликаты (отсутствуют)  
#     * просмотр первых 5 строк и названия столбцов 
# В распоряжении имеетя база данных:
# ![image.png](attachment:image.png)
# 
# 
#     

# <a id="2"></a>
# ## Шаг. Проверка работоспособности БД

# <a id="21"></a> 
# ### Количество книг, которые вышли после 1 января 2000 года

# In[ ]:


query = """
SELECT COUNT(*)
FROM books
WHERE CAST(publication_date AS date) > '2000-01-01'
;
"""

pd.io.sql.read_sql(query, con=engine)


# <div class="alert alert-info" style="background:#ffdbf1;color:#2e00ab;border-left:7px solid #2e00ab">
# <b>Комментарий студента №0</b>
#     
# Вопрос первый: есть ли существенна разница между запросом выше и 
#     
#     
#     """
#     SELECT COUNT(publication_date)
#     FROM books
#     WHERE CAST(publication_date AS date) > '2000-01-01'
#     ;
#     """
# ?
#     
# в тренажере было написано, что вывод запроса имеет вес и чем он меньше(вес) тем лучше, тут мы выводим и первым и вторым запросом всего одну ячейку 
# </div>

# <a id="22"></a> 
# ### Количество обзоров каждой книги и ее средняя оценка

# In[ ]:


query = """
SELECT b.book_id,
       COUNT(review_id) as колво_обзоров,
       AVG(rating) as средняя_оценка       
FROM books b
    JOIN ratings ra ON b.book_id = ra.book_id
    JOIN reviews re ON b.book_id = re.book_id    
GROUP BY b.book_id
;
"""

pd.io.sql.read_sql(query, con=engine)


# <div class="alert alert-info" style="background:#ffdbf1;color:#2e00ab;border-left:7px solid #2e00ab">
# <b>Комментарий студента №0</b>
# 
# Во втором запросе я не указывал таблицы для review_id и rating так как в других таблицах нет таких столбцов.
# 
# Вопрос второй: стоит взять за привычку всегда указавать название таблиц из которых я беру колонки? (re.review_id и ra.rating)
#     
# </div>

# <a id="23"></a> 
# ### Издательство, которое выпустило наибольшее число книг толще 50 страниц

# In[ ]:


query = """
WITH pub AS (
    SELECT publisher,
       count(book_id)       
    FROM books b
        JOIN publishers p ON b.publisher_id = p.publisher_id     
    WHERE b.num_pages > 50
    GROUP BY publisher
    ORDER BY COUNT DESC)

SELECT publisher
FROM pub
LIMIT 1

;
"""

pd.io.sql.read_sql(query, con=engine)


# <a id="24"></a> 
# ### Автор с самой высокой средней оценкой книг(учитывая только книги с 50 и более оценками)

# In[ ]:


query = """

WITH temp AS
    (SELECT b.book_id,
        COUNT(rating_id),
        AVG(rating),
        b.author_id
    FROM books b
        JOIN ratings r ON b.book_id = r.book_id
    GROUP BY b.book_id)
    
SElECT author
FROM temp t
    JOIN authors a ON t.author_id = a.author_id 
WHERE count > 49
ORDER BY avg DESC
LIMIT 1
;
"""

pd.io.sql.read_sql(query, con=engine)


# <a id="25"></a> 
# ### Среднее количество обзоров от пользователей, которые поставили больше 50 оценок

# In[ ]:


#count_ratings - пользователь и кол-во его оценок
#users - пользователи которые поставили больше 50 оценок
#count_reviews - users + колво их обзоров
query = """
WITH count_ratings AS(
    SELECT username,
       COUNT(rating_id)   
    FROM ratings
    GROUP BY username
    ),
    
    users AS (
    SELECT username
    FROM count_ratings
    WHERE count > 50
    ),
    
    count_reviews AS(
    SELECT COUNT(*),
       u.username
    FROM users u
        JOIN reviews r ON u.username = r.username
    GROUP BY u.username
    )
    
SELECT ROUND(AVG(count),2)
FROM count_reviews
    

;
"""

pd.io.sql.read_sql(query, con=engine)


# <a id="3"></a> 
# ## Шаг. Вывод

# <div class="alert alert-info" style="background:#ffdbf1;color:#2e00ab;border-left:7px solid #2e00ab">
# <b>Комментарий студента №0</b>
# 
# Вопрос третий в задаче прокта нужно было 
# 
#     Опишите выводы по каждой из решённых задач.
# 
# По структуре проекта это бы выглядело так :
#     
#     2.1 Количество книг, которые вышли после 1 января 2000 года
#     819
#     В базе данных храниться 819 книг которые вышли после 1.01.2000
#     
# Как по мне так делать не стоит, поэтому предлагаю общий вывод
#     
# </div>

# В ходе выполнения проекта мы выяснили
# 
#     1) БД работает исправно, запросы выполняются, связи между таблицами настроены корректно.
#     2) В базе хранятся 819книг вышедших в 21веке.
#     3) 994 книги имеют хотя бы один отзыв и одну оценку.
#     4) Больше всего книг, которые есть в нашей библиотеке, выпустило издательство 'Penguin Books'.
#     5) Писательница J.K. Rowling и иллюстратор Mary GrandPré за свои книги награждены самой высокой средней оценкой. 
#     6) Читатели которые ставят много оценок (больше 50), часто пишут обзоры, в среднем по 24,3 шт на пользователя.
# 
