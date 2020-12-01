from os import listdir
from os.path import isfile, join
import pathlib
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import requests
BASE_URL = "http:/localhost:8888/"
#response = requests.get(BASE_URL+"get_magic")
#print(response.json())
import re
path = pathlib.Path().absolute()
file_inside_folder = [ a for a in listdir(path) if isfile(join(path,a))]
print(path)
print(file_inside_folder)
readable_inside_folder = [r for r in file_inside_folder if r.endswith(".py")]
print(readable_inside_folder)
readable_inside_folder =['a.txt','c.pdf','k.yml','r.jpg','y.mp3']
file_df = pd.DataFrame(readable_inside_folder)
file_df.columns =['file_name']
newdf = file_df['file_name'].str.split('.',n=5 ,expand= True)
file_df['extension']=newdf[0]+']'
print(file_df)
print(newdf)
print(pd.Timestamp(datetime.now()).strftime("%Y%m%d%H%M"))
conn =  create_engine('mysql+pymysql://sadheesh:password@localhost/information_schema')
resu = pd.read_sql_query("select engine from engines limit 2",con=conn)
#latest_df = pd.read_sql_query("select * from file_monitoring where batch_name=",)
print(resu['engine'][0])


dataframe1 = pd.DataFrame(data={"column1": [1, 2, 3, 4, 5]})
dataframe2 = pd.DataFrame(data={"column1": [1, 2,6]})
common = dataframe1.merge(dataframe2, on=["column1"])
result = dataframe2[~dataframe2.column1.isin(common.column1)]
print(result)
df = pd.DataFrame({'c1': [10, 11, 12], 'c2': [100, 110, 120]})
df = pd.DataFrame({'c1': [10, 11, 12], 'c2': [100, 110, 120]})

for index, row in df.iterrows():
    print(row['c1'], row['c2'])#file_df['extension'] =file_df['file_name'].str.split(1)
'''pattern = re.compile('.*hai*')
magic_string_list = ['hai','hello', 'how', 'are', 'you']
#if any((match := pattern.match(x)) for x in magic_string_list):
#    print(match.group(0))
readed = "hai how are you im fine"
aa= re.findall(r"(?=("+'|'.join(magic_string_list)+r"))",readed)
#print(aa)

st = "['hai','hello', 'how', 'are', 'you'] 15"
ns = st.split("]",-1)
print(ns[0])'''

from configparser import SafeConfigParser

parser = SafeConfigParser()
parser.read("C:\\Users\\sadheesh.v\\PycharmProjects\\vbi_file_api\\venv\\vbi_config.ini")
print(parser.get('magic', 'magic_string'))
