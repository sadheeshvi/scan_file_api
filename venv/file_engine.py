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
      '''
      This module is to track the list of file from given path 
      '''
        try:
            file_inside_folder = [a for a in listdir(folder_path) if isfile(join(folder_path, a))]
            print(file_inside_folder)
            self.process_files(file_inside_folder)
            print("try")
        except Exception as e:
            print("error in file reading ( in folder track module)"+ str(e))


    def process_files(self, file_list_to_process):
      ''' 
      This module will create data frames and search for the magic string in every file 
      also it performs data comparison between current task and previous taks
      '''
        try:
            file_df = DataFrame(file_list_to_process,columns=['file_name'])  # creating a base dataframe based on list of files
            ext_find = file_df['file_name'].str.split('.', n=1, expand=True) # Finding extension of the file 
            file_df['extension'] = ext_find[1]  #  populating extension in one column of data frame
            file_df['magic_values'] = file_df['file_name'].map(lambda element: self.find_magic(element)) # doing string search by calling find_magic function
            magic_count = file_df['magic_values'].str.split(']', n=-1, expand=True) # pharsing the count ( number of magic values present ) and actual string from find_magic output
            file_df['magic_count'] = magic_count[1] # setting magic count in the one column of dataframe
            file_df['magic_string'] = magic_count[0] + ']' # setting the magic string values in one column of dataframe
            file_df["processed_time"] = pd.Timestamp.now() # adding the processing time
            file_df['batch_number'] = pd.Timestamp.now().strftime("%Y%m%d%H%M%S") # adding  batch name - this will be uniq since this is basd on current execution time
            file_df['is_newly_added'] = '' # adding place holder for newly added flag
            file_df['is_deleted'] = '' # adding place holder for newly deleted flag
            self.data_compare_c()  # calling data compare module - this module with compare the current batch with previous batch 
            file_df.to_sql('file_processing', con=self.con, if_exists='append', chunksize=1000) # pushing the data frame into sql 
            
        except Exception as e :
            print("error in file processing ( in file process module " + str(e))


    def find_magic(self, filename):
        try:
            full_file = "C:/Users/sadheesh.v/PycharmProjects/vbi_file_api/venv/vbi_input_files/"+filename # acutal folder in which we need to scan files
            print(full_file)
            with open(full_file, 'rt') as inp_file:  #opening file 
                content = inp_file.read() # reading file
                magic_string_list = ['hai', 'hello', 'how', 'are', 'you']  #  setting magic string or (can be imported from config also ) 
                magic_found_in_file = re.findall(r"(?=(" + '|'.join(magic_string_list) + r"))", content) # finding the magic string in file content using regex
                magic_occurences = len(magic_found_in_file) # getting the count of magic string occurences
            return (str(magic_found_in_file) + str(magic_occurences)) #returnng values 
        except Exception as e:
            print("Error in find magic moduel ( while doing pattern match)"+ str(e))


    def data_compare_c(self):
        try:
            recentbatches = pd.read_sql_query("select batch_number from file_processing group by 1 order by 1 desc limit 2",
                                              con=self.con) # picking last two batches for compare
            print(recentbatches)
            print(f"select * from file_processing where batch_number={recentbatches['batch_number'][0]};")   
            print(f"select * from file_processing where batch_number={recentbatches['batch_number'][1]};")   
            latest_df = pd.read_sql_query(f"select * from file_processing where batch_number={recentbatches['batch_number'][0]};",  con = self.con) # reading data for latest batch
            previous_df = pd.read_sql_query(f"select * from file_processing where batch_number={recentbatches['batch_number'][1]};",  con = self.con) # reading data for previous batch
            print(latest_df)
            print(previous_df)
            common = latest_df.merge(previous_df, on=["file_name"]) #creating common data ( for data comparision)
            newly_added = latest_df[~latest_df.file_name.isin(common.file_name)]  # finding newly added files alone.. using the df comparision
            newly_deleted = previous_df[~previous_df.file_name.isin(common.file_name)] # finding newly deleted files alone.. using the df comparision
            for i,row in newly_added.iterrows(): # updating the filas for newly added files
                update_query_newly_added = f"update file_processing set is_newly_added=1 where batch_number='{recentbatches['batch_number'][0]}' and file_name='{row[1]}';"
                print(update_query_newly_added)
                self.con.execute(update_query_newly_added)
            for i , row in newly_deleted.iterrows(): # update the files for newly deleted
                update_statement_for_newly_deleted = f"update file_processing set is_newly_added=1 where batch_number='{recentbatches['batch_number'][1]}' and file_name='{row[1]}';"
                print(update_statement_for_newly_deleted)
                self.con.execute(update_statement_for_newly_deleted)
            return {'newly_added': newly_added.to_json() , 'newly_deleted': newly_deleted.to_json() , 'recent_batch':latest_df.to_json() , 'older_batch':previous_df.to_json()} #retruning values
            
        except Exception as e:
            print("Exception during data comparision for newly deleted and added fiels ( in data compare module) "+ str(e))


    def set_magic(self, magic_string_to_update):
      '''
      this  will set the magic string values in config file
      '''

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
        '''
        this will set the interval for batch
        '''
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

