"""
Creator: khanh.brandy
Created on 2021-05-30
"""
import sys
from time import strftime
from flask import Flask, render_template, flash, request, abort, jsonify
from wtforms import Form, TextField, TextAreaField, validators, StringField, SubmitField
import os.path
from datetime import datetime
from dateutil import parser
import pickle
import pandas as pd 
from sources.searchproduct import ProductCLF

class ReusableForm(Form):
    name = TextField('Item name:', validators=[validators.required()])

class SearchAPI():
    '''
    Search API: looks for similar products
    
    '''
    def __init__(self):
        self.app = Flask(__name__)
        self.app.config.from_object(__name__)
        self.product_clf = ProductCLF()
        self.app.config["DEBUG"] = True
        self.app.config['development'] = True
        self.app.config['JSON_AS_ASCII'] = False
        self.app.config['JSON_SORT_KEYS'] = False
        self.app.debug = True
        self.app.config['SECRET_KEY'] = 'HIDDEN'
        @self.app.after_request
        def after_request(response):
            response.headers['Access-Control-Allow-Origin'] = "*"
            response.headers['Access-Control-Allow-Methods'] = 'PUT,GET,POST,DELETE'
            response.headers['Access-Control-Allow-Headers'] = 'Content-Type,Authorization'
            return response
        @self.app.route('/')
        def home():
            return '''
                <div>
                    <h1> 
                        <center>
                            <br><br><br><br><br>
                            <a href="/search">
                            
                            Click to continue
                            </a>
                        </center>
                    </h1>
                </div>
                    '''

        @self.app.route('/search', methods = ['POST', 'GET'])
        def search():
            form = ReusableForm(request.form)
            headings = ['Item name', 'Code/ID', 'Price', 'Quantity' ,'Date', 'Score']
            data = []
            if request.method == 'POST':
                name=request.form['name']
                if form.validate():
                    # Write log:
                    # self.writeResult(name)
                    # Search from ES:
                    query = name.lower()
                    res = self.product_clf.search(query)
                    # Process results
                    n = len(res)
                    for i in range(len(res)):
                        business_date_object = parser.parse(res[i]['business_date'])
                        business_date_str = business_date_object.strftime("%Y-%m-%d")
                        data.append((res[i]['product_name'],res[i]['barcode'], int(res[i]['price']), int(res[i]['quantity']), business_date_str, res[i]['score'] ))

                    flash('{}'.format(name), 'Product Name')
                    
                else:
                    flash('Item name is required', 'Product Name')

            return render_template('index.html', form=form, headings = headings, data=data)

            
            # return jsonify(res)
    def getTime(self):
        time = strftime("%Y-%m-%d, %H:%M:%S")
        return time

    def writeResult(self, response):
        data = open('file.log', 'a')
        timestamp = self.getTime()
        data.write('Time= {}, Results= {} \n'.format(timestamp, response))
        data.close()

    def run(self, port):
        # self.app.run(host='0.0.0.0',port=port)
        self.app.run(host='localhost', port=port)

if __name__=='__main__':
    args = sys.argv
    if len(args) > 1:
        port = int(args[1])
    else:
        port = 8080
    service = SearchAPI()
    service.run(port)

