from flask import request, jsonify
import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine ,update
import sqlalchemy , sys
import db_config as cc
import logging, traceback
from os import listdir
from os.path import isfile, join
from configparser import SafeConfigParser
import os
import re

# engine = create_engine('mysql://root:password@localhost/vbi')

logging.basicConfig(filename='./file_Monitoring.log', level=logging.INFO,
                    format="%(asctime)s: %(levelname)s -- %(message)s", datefmt="%Y-%m-%d %H:%M:%S")


class folder_monitor():  ### Main Class
    def __init__(self):  ### DB connection initialization

        try:
            self.con = create_engine('mysql+pymysql://' + str(cc.DB_Credential["username"]) + ':' + str(
                cc.DB_Credential["password"]) + '@' + str(cc.DB_Credential["server"]) + ':3306/' + str('vbi'))
            logging.info("DB connection in init")
            basepath = "C:/Users/sadheesh.v/PycharmProjects/vbi_file_api/venv/vbi_input_files/"
        except:
            logging.error("Error in connection to db")

    def folder_track(self, folder_path):
        try:
            file_inside_folder = [a for a in listdir(folder_path) if isfile(join(folder_path, a))]
            print(file_inside_folder)
            self.process_files(file_inside_folder)
            print("try")
        except Exception as e:
            print("error in file reading ( in folder track module)"+ str(e))


    def process_files(self, file_list_to_process):
        try:
            file_df = DataFrame(file_list_to_process,columns=['file_name'])
            ext_find = file_df['file_name'].str.split('.', n=1, expand=True)
            file_df['extension'] = ext_find[1]  # $$$$$$$$$$$$$$$$$$$$$
            # file_df = file_df.assign(magic_values = lambda x:(x[file_df['file_name']]))
            # df = df.assign(Percentage=lambda x: (x['Total_Marks'] / 500 * 100))
            # df['hour'] = df.apply(lambda x: find_hour(x['Dates']), axis=1)
            #file_df['magic_values'] = file_df.apply(lambda x: self.find_magic(x['file_name'], axis=1))
            file_df['magic_values'] = file_df['file_name'].map(lambda element: self.find_magic(element))
            magic_count = file_df['magic_values'].str.split(']', n=-1, expand=True)
            file_df['magic_count'] = magic_count[1]
            file_df['magic_string'] = magic_count[0] + ']'
            file_df["processed_time"] = pd.Timestamp.now()
            file_df['batch_number'] = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
            file_df['is_newly_added'] = ''
            file_df['is_deleted'] = ''
            self.data_compare_c()
            file_df.to_sql('file_processing', con=self.con, if_exists='append', chunksize=1000)
            print("trying")
        except Exception as e :
            print("error in file processing ( in file process module " + str(e))


    def find_magic(self, filename):
        try:
            full_file = "C:/Users/sadheesh.v/PycharmProjects/vbi_file_api/venv/vbi_input_files/"+filename
            print(full_file)
            with open(full_file, 'rt') as inp_file:
                content = inp_file.read()
                magic_string_list = ['hai', 'hello', 'how', 'are', 'you']  # or can be imported from config
                magic_found_in_file = re.findall(r"(?=(" + '|'.join(magic_string_list) + r"))", content)
                magic_occurences = len(magic_found_in_file)
            return (str(magic_found_in_file) + str(magic_occurences))
        except Exception as e:
            print("Error in find magic moduel ( while doing pattern match)"+ str(e))


    def data_compare_c(self):
        try:
            recentbatches = pd.read_sql_query("select batch_number from file_processing group by 1 order by 1 desc limit 2",
                                              con=self.con)
            print(recentbatches)
            print(f"select * from file_processing where batch_number={recentbatches['batch_number'][0]};")
            print(f"select * from file_processing where batch_number={recentbatches['batch_number'][1]};")
            latest_df = pd.read_sql_query(f"select * from file_processing where batch_number={recentbatches['batch_number'][0]};",  con = self.con)
            previous_df = pd.read_sql_query(f"select * from file_processing where batch_number={recentbatches['batch_number'][1]};",  con = self.con)
            print(latest_df)
            print(previous_df)
            common = latest_df.merge(previous_df, on=["file_name"])
            newly_added = latest_df[~latest_df.file_name.isin(common.file_name)]
            newly_deleted = previous_df[~previous_df.file_name.isin(common.file_name)]
            print("newly added files")
            print(newly_added)
            print("newly deleted files")
            print(newly_deleted)
            for i,row in newly_added.iterrows():
                update_query_newly_added = f"update file_processing set is_newly_added=1 where batch_number='{recentbatches['batch_number'][0]}' and file_name='{row[1]}';"
                print(update_query_newly_added)
                self.con.execute(update_query_newly_added)
            for i , row in newly_deleted.iterrows():
                update_statement_for_newly_deleted = f"update file_processing set is_newly_added=1 where batch_number='{recentbatches['batch_number'][1]}' and file_name='{row[1]}';"
                print(update_statement_for_newly_deleted)
                self.con.execute(update_statement_for_newly_deleted)
            return {'newly_added': newly_added.to_json() , 'newly_deleted': newly_deleted.to_json() , 'recent_batch':latest_df.to_json() , 'older_batch':previous_df.to_json()}
            '''
            update_statement_for_newly_added = sqlalchemy.update(file_processing).where(batch_number==recentbatches['batch_number'][0]).values(is_newly_added=1)
            print(update_statement_for_newly_added)
            print(str(update_statement_for_newly_added.compile(dialect=sqlalchemy.dialects.mysql.dialect())))
            self.con.execute(update_statement_for_newly_added)

            update_statement_for_newly_deleted = sqlalchemy.update(file_processing).where(batch_number==recentbatches['batch_number'][1]).values(is_newly_deleted=1)
            '''
        except Exception as e:
            print("Exception during data comparision for newly deleted and added fiels ( in data compare module) "+ str(e))


    def set_magic(self, magic_string_to_update):


        try:
            parser = SafeConfigParser()
            parser.read("C:\\Users\\sadheesh.v\\PycharmProjects\\vbi_file_api\\venv\\vbi_config.ini")
            print(parser.get('magic', 'magic_string'))
            parser.set('magic', 'magic_string', magic_string_to_update)
            with open("C:\\Users\\sadheesh.v\\PycharmProjects\\vbi_file_api\\venv\\vbi_config.ini", 'w') as configfile:
                parser.write(configfile)
            return {'data':f'{magic_string_to_update} updated'}


        except Exception as e :
            print(f'exception in setting magic string (set_magic) {e}')


    def set_batch_interval(self,  type_val):
        try:
            parser = SafeConfigParser()
            parser.read("C:\\Users\\sadheesh.v\\PycharmProjects\\vbi_file_api\\venv\\vbi_config.ini")
            print(parser.get('Interval', 'batch_process'))
            parser.set('Interval', 'batch_process', type_val)
            with open("C:\\Users\\sadheesh.v\\PycharmProjects\\vbi_file_api\\venv\\vbi_config.ini", 'w') as configfile:
                parser.write(configfile)
            return {'data': f'{type_val} updated'}
        except Exception as e:
            print(f"exception in setting interval (set_interval module) {e}")


if __name__ == '__main__':
    obj = folder_monitor()
    obj.folder_track("C:/Users/sadheesh.v/PycharmProjects/vbi_file_api/venv/vbi_input_files/")
    obj.data_compare_c()

