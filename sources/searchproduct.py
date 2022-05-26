import pandas as pd
import numpy as np
import multiprocessing
from multiprocessing import Pool
from functools import partial
from elasticsearch import Elasticsearch
# from underthesea import word_tokenize
import time
import json


class ProductCLF():
    def __init__(self):
        self.es_client = Elasticsearch(hosts=[{"host": "HIDDEN", "port": 41039}]) #Hidden
        self.es_index = 'index_name' #Hidden

    def after_request(self, response):
        response.headers['Access-Control-Allow-Origin'] = "*"
        response.headers['Access-Control-Allow-Methods'] = 'PUT,GET,POST,DELETE'
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type,Authorization'
        return response

    
    def removePunctuation(self, inputString):
        # Remove punctuations 
        inputString = str(inputString)
        punctuation = u'=_~`!@#$%^*()+{}[]|;,.:"<>?"&/□□' # do not contain "-"
        _clean = lambda x: "".join(" " if i in punctuation else i for i in x.strip(punctuation))
        s = " ".join(_clean(i) for i in inputString.split())
        # Replace double space ( or tab )  by 1 space and remove '-'
        s = s.replace('-','').replace('  ', ' ')
        return s

    def removeAccents(self, inputString):
        # Remove accents
        inputString = str(inputString)
        s1 = u'ÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚÝàáâãèéêìíòóôõùúýĂăĐđĨĩŨũƠơƯưẠạẢảẤấẦầẨẩẪẫẬậẮắẰằẲẳẴẵẶặẸẹẺẻẼẽẾếỀềỂểỄễỆệỈỉỊịỌọỎỏỐốỒồỔổỖỗỘộỚớỜờỞởỠỡỢợỤụỦủỨứỪừỬửỮữỰựỲỳỴỵỶỷỸỹ'
        s0 = u'AAAAEEEIIOOOOUUYaaaaeeeiioooouuyAaDdIiUuOoUuAaAaAaAaAaAaAaAaAaAaAaAaEeEeEeEeEeEeEeEeIiIiOoOoOoOoOoOoOoOoOoOoOoOoUuUuUuUuUuUuUuYyYyYyYy'
        s = ''
        for c in inputString:
            if c in s1:
                s += s0[s1.index(c)]
            else:
                s += c
        return s.lower()

    def hasNumbers(self, inputString):
        # Check numeric characters
        return any(char.isdigit() for char in inputString)

    def removeNumbers(self, inputString):
        # Remove numeric characters
        return ''.join(i for i in inputString if not i.isdigit())

    def search(self, query, mlt =True):
        category = {
            'level_1': "KHONG THE PHAN LOAI",
            'level_2': "KHONG THE PHAN LOAI",
            'name': query}
        if pd.isna(query):
            return category
        else:
            # Lower product names and cremove punctuations
            query = self.removePunctuation(query)
            query = self.removeNumbers(query)
            query_vi = str(query)
            params = [
                # Top 10 items matching query_vi (highest tf-idf)
                {'index': self.es_index},
                {
                "query": {
                    "more_like_this" : {
                    "fields" : ["product_name"],
                    "like" : query_vi,
                    "min_term_freq" : 1,
                    "max_query_terms" : 12
                    }
                },
                "from": 0,
                "size": 20
                }
                
            ]


            res = self.es_client.msearch(body=params, request_timeout=60)
            
            ################################################ AGGREGATION ################################################
    
            # Search results for More Like This 
            res_mlt = []
            if len(res["responses"][0]["hits"]["hits"]) > 0:
                for ele in res["responses"][0]["hits"]["hits"]:
                    score = ele["_score"]
                    ele["_source"]['score'] = score
                    res_mlt.append(ele["_source"])
            
            print(res_mlt)
            # print(category)
            return res_mlt    

class ESearch():
    def __init__(self):
        pass

    def poolSearch(self, index, product_names):
        product_clf = ProductCLF()
        return product_clf.search(product_names[index]) 

    def multiSearch(self, product_names):
        indices = list(range(len(product_names)))
        n_process = multiprocessing.cpu_count()*2+1
        pool = Pool(n_process)
        result = pool.map(partial(self.poolSearch, product_names=product_names), indices)
        pool.close()
        pool.join()
        return result

def run(name_source, multi = True):
    start = time.time()
    print('Start searching...')
    if multi:
        es = ESearch()
        # Multi-request
        data = pd.read_csv(name_source+'.csv', header = 0, encoding='utf8')
        product_names = data['product_name']
        result = es.multiSearch(product_names)
        data['result'] = result
        data.to_csv(name_source + '_with_result'+'.csv', index=False, encoding='utf-8-sig')
    else:
        product_clf = ProductCLF()
        # Single-request
        res = product_clf.search(name_source)
        # print(res)
    print('Done getting result. Time taken = {:.1f}(s) \n'.format(time.time()-start))
    
if __name__ == '__main__':
    name_source = 'iphone xs max'
    run(name_source, multi = False)
    
        
    
    


