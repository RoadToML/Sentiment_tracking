import tweepy_search as ts

from flask import Flask, render_template, request
from bokeh.embed import components

application = Flask(__name__)

@application.route('/')
def my_form():
    return render_template('search_form.html')

@application.route('/', methods=['POST'])
def index():
    db_file = 'test2.db'

    search_term = request.form['text']

    #search_term = input('Who would u like to search for?\n')
    ts.collect_tweets(db_file, search_term, result_type = 'popular')

    ts.data_cleaning(db_file, search_term)
    ts.sentiment_analysis(db_file, search_term)

    plot = ts.plotting(db_file, search_term)
    script, div = components(plot)

    return render_template('test_graph.html', search_term = search_term, script=script, div=div)

if __name__ == '__main__':
    application.run()
