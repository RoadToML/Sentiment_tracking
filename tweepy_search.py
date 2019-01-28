<<<<<<< HEAD
from twitter_cred import access_token, access_token_secret, consumer_key, consumer_secret

import json
import sqlite3 as sql
from datetime import datetime, timedelta

from tweepy import OAuthHandler, API
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk import word_tokenize, sent_tokenize
from nltk.corpus import stopwords
from bokeh.plotting import figure
from bokeh.io import show, output_file

def collect_tweets(twitter_search_term):

    conn = sql.connect('tweets.db')
    cur = conn.cursor()

    cur.execute('''CREATE TABLE IF NOT EXISTS tweets
                 (created_at text, keyword text, id integer, username text, followers integer, tweet_text text,\
                      processed_tweet text, sentiment real)''')

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = API(auth, wait_on_rate_limit=True)

    _until_7_days = datetime.today() - timedelta(days = 7)
    format_until_7_days = _until_7_days.strftime('%Y-%m-%d')

    # until = format_until_7_days,\
    arr = api.search(q = twitter_search_term,\
    tweet_mode = 'extended', count = 100, lang = 'en', result_type = 'popular')

    #arr = Cursor(api.search, q = f"from:realDonaldTrump until: {format_until_7_days}, count = 15, lang = 'en', tweet_mode = 'extended' ")
    for single_json_tweet in range(len(arr)):
        convert_json = json.dumps(arr[single_json_tweet]._json)
        tweet = json.loads(convert_json)

        created_at = tweet['created_at']
        tweet_t = tweet['full_text']
        username = tweet['user']['name']
        followers = tweet['user']['followers_count']
        tweet_id = tweet['id']


        cur.execute('''INSERT INTO tweets(created_at, keyword, id, username, followers, tweet_text)\
        VALUES (?, ?, ?, ?, ?, ?)''', (created_at, twitter_search_term, tweet_id, username, followers, tweet_t))
        conn.commit()

    cur.execute('SELECT min(id) from tweets')
    min_id = cur.fetchall()[0][0]
    new_min_id = 0

    while min_id != new_min_id:
        arr = api.search(q = twitter_search_term,\
        tweet_mode = 'extended', count = 100, lang = 'en', max_id = min_id, result_type = 'popular')

        for single_json_tweet in range(len(arr)):
            convert_json = json.dumps(arr[single_json_tweet]._json)
            tweet = json.loads(convert_json)

            created_at = tweet['created_at']
            tweet_t = tweet['full_text']
            username = tweet['user']['name']
            followers = tweet['user']['followers_count']
            tweet_id = tweet['id']


            cur.execute('''INSERT INTO tweets(created_at, keyword, id, username, followers, tweet_text)\
            VALUES (?, ?, ?, ?, ?, ?)''', (created_at, twitter_search_term, tweet_id, username, followers, tweet_t))
            conn.commit()

        cur.execute('SELECT min(id) from tweets')
        new_min_id = min_id
        min_id = cur.fetchall()[0][0]

    conn.close()

def data_cleaning(sql_db_file):

    conn = sql.connect(sql_db_file)
    cur = conn.cursor()
    stopWords = set(stopwords.words('english'))

    cur.execute("""SELECT _rowid_, created_at, tweet_text FROM tweets
    WHERE keyword = ?""",(search_term,))
    sql_db_rows = cur.fetchall()

    for tweet_row in sql_db_rows:

        sent_tokenized_tweet_text = sent_tokenize(tweet_row[2].lower())
        temp_set_word_to_db = []

        for sent in sent_tokenized_tweet_text:
            word_tokenized_tweet_text = word_tokenize(sent)

            word_tokenized_tweet_text = [i for i in word_tokenized_tweet_text if i.isalpha()]

            for word in word_tokenized_tweet_text:
                if word not in stopWords and word not in ['https', 'http']:
                    temp_set_word_to_db.append(word)



        _temp_string_word_to_db = ' '.join(temp_set_word_to_db)

        cur.execute('''UPDATE tweets SET processed_tweet = ? WHERE _rowid_ = ? ;''', (_temp_string_word_to_db, tweet_row[0]))
        conn.commit()

    conn.close()

def sentiment_analysis(sql_db_file):

    conn = sql.connect(sql_db_file)
    cur = conn.cursor()

    cur.execute(f'''SELECT _rowid_, processed_tweet FROM tweets
    WHERE keyword = ?''', (search_term,))
    all_processed_tweets = cur.fetchall()

    sid = SentimentIntensityAnalyzer()
    for tweet in all_processed_tweets:
        ss = sid.polarity_scores(tweet[1])
        cur.execute('''UPDATE tweets SET sentiment = ? WHERE _rowid_ = ?''', (ss['compound'], tweet[0]))
        conn.commit()

    conn.close()

def plotting(sql_db_file):

    conn = sql.connect(sql_db_file)
    cur = conn.cursor()

    cur.execute(f'''Select substr(created_at, 9, 2) || '-' ||\
    substr(created_at, 5, 3) || '-' || substr(created_at, 27, 4)\
    as date, avg(sentiment) as sentiment from tweets where keyword = ? group by 1''', (search_term,))


    all_tweets = cur.fetchall()

    output_file('sentiment_over_time.html')

    _dates_to_plot = [datetime.strptime(i, '%d-%b-%Y') for i, j in all_tweets]
    _sentiment_to_plot = [j for i, j in all_tweets]

    p = figure(title = 'Daily sentiment over 1 week', plot_width = 500, plot_height = 500, x_axis_type = 'datetime')
    p.line(_dates_to_plot, _sentiment_to_plot, line_width = 2)

    show(p)


if __name__ == '__main__':

    search_term = input('Who would u like to search for?\n')
    collect_tweets(search_term)

    data_cleaning('tweets.db')
    sentiment_analysis('tweets.db')
    plotting('tweets.db')






"""class StdOutListener(StreamListener):

    def on_data(self, data):
        print(data)

        return True

    def on_error(self, status):
        print(status)

if __name__ == '__main__':

    listener = StdOutListener()


    stream = Stream(auth, listener)

    stream.filter(track=['carrot', 'banana'])"""
=======
from twitter_cred import access_token, access_token_secret, consumer_key, consumer_secret

import json
import sqlite3 as sql
from datetime import datetime, timedelta

from tweepy import OAuthHandler, API
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk import word_tokenize, sent_tokenize
from nltk.corpus import stopwords
from bokeh.plotting import figure
from bokeh.io import show, output_file

def collect_tweets(twitter_search_term):

    conn = sql.connect('tweets.db')
    cur = conn.cursor()

    cur.execute('''CREATE TABLE IF NOT EXISTS tweets
                 (created_at text, keyword text, id integer, username text, followers integer, tweet_text text,\
                      processed_tweet text, sentiment real)''')

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = API(auth, wait_on_rate_limit=True)

    _until_7_days = datetime.today() - timedelta(days = 7)
    format_until_7_days = _until_7_days.strftime('%Y-%m-%d')

    # until = format_until_7_days,\
    arr = api.search(q = twitter_search_term,\
    tweet_mode = 'extended', count = 100, lang = 'en', result_type = 'popular')

    #arr = Cursor(api.search, q = f"from:realDonaldTrump until: {format_until_7_days}, count = 15, lang = 'en', tweet_mode = 'extended' ")
    for single_json_tweet in range(len(arr)):
        convert_json = json.dumps(arr[single_json_tweet]._json)
        tweet = json.loads(convert_json)

        created_at = tweet['created_at']
        tweet_t = tweet['full_text']
        username = tweet['user']['name']
        followers = tweet['user']['followers_count']
        tweet_id = tweet['id']


        cur.execute('''INSERT INTO tweets(created_at, keyword, id, username, followers, tweet_text)\
        VALUES (?, ?, ?, ?, ?, ?)''', (created_at, twitter_search_term, tweet_id, username, followers, tweet_t))
        conn.commit()

    cur.execute('SELECT min(id) from tweets')
    min_id = cur.fetchall()[0][0]
    new_min_id = 0

    while min_id != new_min_id:
        arr = api.search(q = twitter_search_term,\
        tweet_mode = 'extended', count = 100, lang = 'en', max_id = min_id, result_type = 'popular')

        for single_json_tweet in range(len(arr)):
            convert_json = json.dumps(arr[single_json_tweet]._json)
            tweet = json.loads(convert_json)

            created_at = tweet['created_at']
            tweet_t = tweet['full_text']
            username = tweet['user']['name']
            followers = tweet['user']['followers_count']
            tweet_id = tweet['id']


            cur.execute('''INSERT INTO tweets(created_at, keyword, id, username, followers, tweet_text)\
            VALUES (?, ?, ?, ?, ?, ?)''', (created_at, twitter_search_term, tweet_id, username, followers, tweet_t))
            conn.commit()

        cur.execute('SELECT min(id) from tweets')
        new_min_id = min_id
        min_id = cur.fetchall()[0][0]

    conn.close()

def data_cleaning(sql_db_file):

    conn = sql.connect(sql_db_file)
    cur = conn.cursor()
    stopWords = set(stopwords.words('english'))

    cur.execute("""SELECT _rowid_, created_at, tweet_text FROM tweets
    WHERE keyword = ?""",(search_term,))
    sql_db_rows = cur.fetchall()

    for tweet_row in sql_db_rows:

        sent_tokenized_tweet_text = sent_tokenize(tweet_row[2].lower())
        temp_set_word_to_db = []

        for sent in sent_tokenized_tweet_text:
            word_tokenized_tweet_text = word_tokenize(sent)

            word_tokenized_tweet_text = [i for i in word_tokenized_tweet_text if i.isalpha()]

            for word in word_tokenized_tweet_text:
                if word not in stopWords and word not in ['https', 'http']:
                    temp_set_word_to_db.append(word)



        _temp_string_word_to_db = ' '.join(temp_set_word_to_db)

        cur.execute('''UPDATE tweets SET processed_tweet = ? WHERE _rowid_ = ? ;''', (_temp_string_word_to_db, tweet_row[0]))
        conn.commit()

    conn.close()

def sentiment_analysis(sql_db_file):

    conn = sql.connect(sql_db_file)
    cur = conn.cursor()

    cur.execute(f'''SELECT _rowid_, processed_tweet FROM tweets
    WHERE keyword = ?''', (search_term,))
    all_processed_tweets = cur.fetchall()

    sid = SentimentIntensityAnalyzer()
    for tweet in all_processed_tweets:
        ss = sid.polarity_scores(tweet[1])
        cur.execute('''UPDATE tweets SET sentiment = ? WHERE _rowid_ = ?''', (ss['compound'], tweet[0]))
        conn.commit()

    conn.close()

def plotting(sql_db_file):

    conn = sql.connect(sql_db_file)
    cur = conn.cursor()

    cur.execute(f'''Select substr(created_at, 9, 2) || '-' ||\
    substr(created_at, 5, 3) || '-' || substr(created_at, 27, 4)\
    as date, avg(sentiment) as sentiment from tweets where keyword = ? group by 1''', (search_term,))


    all_tweets = cur.fetchall()

    output_file('sentiment_over_time.html')

    _dates_to_plot = [datetime.strptime(i, '%d-%b-%Y') for i, j in all_tweets]
    _sentiment_to_plot = [j for i, j in all_tweets]

    p = figure(title = 'Daily sentiment over 1 week', plot_width = 500, plot_height = 500, x_axis_type = 'datetime')
    p.line(_dates_to_plot, _sentiment_to_plot, line_width = 2)

    show(p)


if __name__ == '__main__':

    search_term = input('Who would u like to search for?\n')
    collect_tweets(search_term)

    data_cleaning('tweets.db')
    sentiment_analysis('tweets.db')
    plotting('tweets.db')






"""class StdOutListener(StreamListener):

    def on_data(self, data):
        print(data)

        return True

    def on_error(self, status):
        print(status)

if __name__ == '__main__':

    listener = StdOutListener()


    stream = Stream(auth, listener)

    stream.filter(track=['carrot', 'banana'])"""
>>>>>>> 32a1826c1a65a493d3d444ada0becdf1761d9f27
