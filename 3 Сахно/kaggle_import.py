import csv
import decimal
import psycopg2
import psycopg2.extras
username = 'postgres'
password = '****'
database = 'Dasha_DB'
host = 'localhost'
port = '5432'

INPUT_CSV_FILE = 'IMDBTop50.csv'

query_1 = '''
INSERT INTO movies (m_name, id_rating, id_genre, id_runtime) VALUES (%s, %s, %s, %s)
'''

query_2 = '''
INSERT INTO rating (id_rating, m_rating ) VALUES (%s, %s)
'''

query_3 = '''
CREATE TABLE tab12 AS (SELECT ROW_NUMBER() OVER(ORDER BY m_rating) AS id_rating, m_rating FROM rating GROUP BY m_rating)
'''

query_4 = '''
DROP TABLE tab12 
'''
query_5 = '''
DELETE FROM rating 
'''

query_6 = '''
INSERT INTO rating select id_rating, m_rating from tab12
'''

query_7 = '''
INSERT INTO genre (id_genre, genre_name) VALUES (%s, %s)
'''

query_8 = '''
CREATE TABLE newgenre AS (SELECT ROW_NUMBER() OVER(ORDER BY genre_name) AS id_genre, genre_name FROM genre GROUP BY genre_name)
'''

query_9 = '''
DROP TABLE newgenre 
'''
query_10 = '''
DELETE FROM genre 
'''

query_11 = '''
INSERT INTO genre select id_genre, genre_name from newgenre
'''

query_13 = '''
INSERT INTO runtime (id_runtime, runtime) VALUES (%s, %s)
'''

query_14 = '''
CREATE TABLE newruntime AS (SELECT ROW_NUMBER() OVER(ORDER BY runtime) AS id_runtime, runtime FROM runtime GROUP BY runtime)
'''

query_15 = '''
DROP TABLE newruntime
'''
query_16 = '''
DELETE FROM runtime
'''

query_17 = '''
INSERT INTO runtime select id_runtime, runtime from newruntime
'''

conn = psycopg2.connect(user=username, password=password, dbname=database)

with conn:
    cur = conn.cursor()
    cur2 = conn.cursor()
    cur3 = conn.cursor()
    cur4 = conn.cursor()
    cur5 = conn.cursor()

    cur6 = conn.cursor()
    cur7 = conn.cursor()
    cur8 = conn.cursor()
    cur9 = conn.cursor()
    cur10 = conn.cursor()
    cur11 = conn.cursor()
    #cur2.execute(query_5)
    #cur3.execute(query_4)

    with open(INPUT_CSV_FILE, 'r') as inf:
        reader = csv.DictReader(inf, delimiter=',')
        for idx, row in enumerate(reader):
            id_rating = idx + 1
            m_rating = row['Rating']
            values = (id_rating, m_rating)
            cur2.execute(query_2, values)
            cur3.execute(query_3)
            cur2.execute(query_5)
            cur2.execute(query_6)
            cur3.execute(query_4)
    with open(INPUT_CSV_FILE, 'r') as inf:
        reader = csv.DictReader(inf, delimiter=',')
        for idx, row in enumerate(reader):
            id_genre = idx + 1
            genre_name = row['Genre']
            values3 = (id_genre, genre_name)
            cur6.execute(query_7, values3)
            cur7.execute(query_8)
            cur6.execute(query_10)
            cur6.execute(query_11)
            cur7.execute(query_9)
    with open(INPUT_CSV_FILE, 'r') as inf:
        reader = csv.DictReader(inf, delimiter=',')
        for idx, row in enumerate(reader):
            id_runtime = idx + 1
            runtime = row['Runtime']
            values3 = (id_runtime, runtime)
            cur10.execute(query_13, values3)
            cur11.execute(query_14)
            cur10.execute(query_16)
            cur10.execute(query_17)
            cur11.execute(query_15)
    with open(INPUT_CSV_FILE, 'r') as inf:
        reader = csv.DictReader(inf, delimiter=',')
        for idx, row in enumerate(reader):
            m_name = row['Title']

            id_genre = row['Genre']
            if not id_genre:
                continue
            cur5.execute('select id_genre from genre where genre_name = %s', (id_genre,))
            id_genre = cur5.fetchone()[0]

            id_rating = row['Rating']
            if not id_rating:
                continue
            cur9.execute('select id_rating from rating where m_rating = %s', (id_rating,))
            id_rating = cur9.fetchone()[0]

            id_runtime = row['Runtime']
            if not id_runtime:
                continue
            cur4.execute('select id_runtime from runtime where runtime = %s', (id_runtime,))
            id_runtime = cur4.fetchone()[0]

            values2 = (m_name, id_rating, id_genre, id_runtime)
            cur.execute(query_1, values2)

    conn.commit()