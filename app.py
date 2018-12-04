#!/usr/bin/python3

'''
This module is a Flask app that implements parsing through TSN.UA and UKR.NET news.
Use URLs written in the @app.route to create HTTP requests.

To run this file type into console:
>$ export FLASK_APP=app.py
>$ flask run
 
'''

from flask import Flask, render_template, request, redirect, url_for, abort
from wtforms import Form, SelectField, StringField, validators
from scraper import Scraper


# initialize flask app
app = Flask(__name__)


# initialize news article Scraper
scraper = Scraper()


class CategoryForm(Form):
    '''
    Form for data input into category field
    '''
    category = StringField('News category', validators=[validators.InputRequired()])


class TokenForm(Form):
    '''
    Form for data input into searched text field
    '''
    text = StringField('Text search', validators=[validators.InputRequired()])


@app.route('/', methods=['GET'])
def index():
    '''
    view for index page with menu options
    '''
    return render_template('index.html')


@app.route('/category_search', methods=['GET', 'POST'])
def category_search():
    '''
    view for news search by input category
    '''
    form = CategoryForm(request.form)
    if request.method == 'POST' and form.validate():
        try:
            category_choice = request.form['category'].encode('utf-8')
            result = scraper.search_by_category(category_choice)
            return render_template('news_result.html', content=result)
        except Exception:
            return render_template('error.html')
    return render_template('category_search.html', form=form)


@app.route('/text_search', methods=['GET', 'POST'])
def text_search():
    '''
    view for news search by input text
    '''
    form = TokenForm(request.form)
    if request.method == 'POST':
        try:
            searched_text = request.form['token'].encode('utf-8')
            result = scraper.search_by_text(searched_text)
            return render_template('news_result.html', content=result)
        except Exception:
            return render_template('error.html')
    return render_template('text_search.html', form=form)


if __name__ == '__main__':
    app.run(debug=True)
