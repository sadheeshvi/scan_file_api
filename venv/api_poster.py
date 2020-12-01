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

api.add_resource(vbi_api,"/runtask/")

@app.route('/')
def hello():
    return "Hello World!"


@app.route('/set_magic/value_list')
def set_magic_list(value_list):
    fe.set_magic(value_list)


@app.route('/see_magic/folderstatus')
def retrive_data():
        pass
    

@app.route('/see_magic/setconfig')
def set_config():
    pass



if __name__ == '__main__':
    app.run(host = 'localhost',port=8888,debug=True)

'''
TODOS = {
    'todo1': {'task': 'build an API'},
    'todo2': {'task': '?????'},
    'todo3': {'task': 'profit!'},
}


def abort_if_todo_doesnt_exist(todo_id):
    if todo_id not in TODOS:
        abort(404, message="Todo {} doesn't exist".format(todo_id))

parser = reqparse.RequestParser()
parser.add_argument('task')


# Todo
# shows a single todo item and lets you delete a todo item
class Todo(Resource):
    def get(self, todo_id):
        abort_if_todo_doesnt_exist(todo_id)
        return TODOS[todo_id]

    def delete(self, todo_id):
        abort_if_todo_doesnt_exist(todo_id)
        del TODOS[todo_id]
        return '', 204

    def put(self, todo_id):
        args = parser.parse_args()
        task = {'task': args['task']}
        TODOS[todo_id] = task
        return task, 201


# TodoList
# shows a list of all todos, and lets you POST to add new tasks
class TodoList(Resource):
    def get(self):
        return TODOS

    def post(self):
        args = parser.parse_args()
        todo_id = int(max(TODOS.keys()).lstrip('todo')) + 1
        todo_id = 'todo%i' % todo_id
        TODOS[todo_id] = {'task': args['task']}
        return TODOS[todo_id], 201

##
## Actually setup the Api resource routing here
##
api.add_resource(TodoList, '/todos')
api.add_resource(Todo, '/todos/<todo_id>')

'''