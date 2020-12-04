from flask_restful import reqparse, abort, Api, Resource
from flask import Flask
import logging
import re
import time
import datetime
import sched
import time
import file_engine as fe
#logging.basicConfig(filename='/tmp/poster.log',level=logging.INFO, format="%(asctime)s: %(levelname)s -- %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

first_res_msg = { "message" : "Request initiated"   }

#schedule.every(10).minutes.do(tracking_folder) # need to be retrived from config
#schedule.every(30).minutes.do(retriave_data) # need to be retrived from config

app = Flask(__name__)
api = Api(app)

vbi_put_args = reqparse.RequestParser()
vbi_put_args.add_argument("task_name", type= str, help="please share the correct name")

class vbi_api(Resource):
    def get(self):
        ob = fe.folder_monitor()
        result = ob.data_compare_c()
        return result
    def post(self,):
        return  {"given task": task , "data" : "under post method of api"}

class set_val(Resource):
    def get(self,value_list):
        ob = fe.folder_monitor()
        ob.set_magic(value_list)
        return {'data': f'values_updated {value_list}'}

class datacompare(Resource):
    def get(self,):
        ob = fe.folder_monitor()
        vals = ob.data_compare_c()
        return  vals

api.add_resource(vbi_api,"/runtask/")
api.add_resource(set_val,"/set_magic/<string:value_list>")
#api.add_resource(set_val,"/set_batch_interval/<int:type_val>")
api.add_resource(datacompare,"/datacompare")

@app.route('/')
def hello():
    return "Hello World!"
    pass


@app.route('/see_magic/folderstatus')
def retrive_data():
        pass
    

@app.route('/see_magic/setconfig')
def set_config():
    pass



if __name__ == '__main__':
    app.run(host = 'localhost',port=8888,debug=True)

