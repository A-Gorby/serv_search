import os, sys
import zipfile
import requests
from urllib.parse import urlencode
import argparse
import datetime
# from g import logger


import pandas as pd
import numpy as np
import os, sys, glob
import humanize
import re
import xlrd

import json
import itertools
#from urllib.request import urlopen
#import requests, xmltodict
import time, datetime
import math
from pprint import pprint
import gc
from tqdm import tqdm
tqdm.pandas()
import pickle

import logging
import zipfile
import warnings
import argparse

from openpyxl import load_workbook
from openpyxl import Workbook
from openpyxl.comments import Comment
from openpyxl.styles import colors
from openpyxl.styles import Font, Color
from openpyxl.utils import units
from openpyxl.styles import Border, Side, PatternFill, GradientFill, Alignment
class Logger():
    def __init__(self, name = 'Fuzzy Lookup',
                 strfmt = '[%(asctime)s] [%(levelname)s] > %(message)s', # strfmt = '[%(asctime)s] [%(name)s] [%(levelname)s] > %(message)s'
                 level = logging.INFO,
                 datefmt = '%H:%M:%S', # '%Y-%m-%d %H:%M:%S'
                #  datefmt = '%H:%M:%S %p %Z',

                 ):
        self.name = name
        self.strfmt = strfmt
        self.level = level
        self.datefmt = datefmt
        self.logger = logging.getLogger(name)
        self.logger.setLevel(self.level) #logging.INFO)
        self.offset = datetime.timezone(datetime.timedelta(hours=3))
        # create console handler and set level to debug
        self.ch = logging.StreamHandler()
        self.ch.setLevel(self.level)
        # create formatter
        self.strfmt = strfmt # '[%(asctime)s] [%(levelname)s] > %(message)s'
        self.datefmt = datefmt # '%H:%M:%S'
        # СЃРѕР·РґР°РµРј С„РѕСЂРјР°С‚С‚РµСЂ
        self.formatter = logging.Formatter(fmt=strfmt, datefmt=datefmt)
        self.formatter.converter = lambda *args: datetime.datetime.now(self.offset).timetuple()
        self.ch.setFormatter(self.formatter)
        # add ch to logger
        self.logger.addHandler(self.ch)
logger = Logger().logger
logger.propagate = False

if len(logger.handlers) > 1:
    for handler in logger.handlers:
        logger.removeHandler(handler)
    # del logger
    logger = Logger().logger
    logger.propagate = False

def unzip_file(path_source, fn_zip, work_path):
    logger.info('Unzip ' + fn_zip + ' start...')

    try:
        with zipfile.ZipFile(path_source + fn_zip, 'r') as zip_ref:
            fn_list = zip_ref.namelist()
            zip_ref.extractall(work_path)
        logger.info('Unzip ' + fn_zip + ' done!')
        return fn_list[0]
    except Exception as err:
        logger.error('Unzip error: ' + str(err))
        sys.exit(2)

def save_df_to_excel(df, path_to_save, fn_main, columns = None, b=0, e=None, index=False):
    offset = datetime.timezone(datetime.timedelta(hours=3))
    dt = datetime.datetime.now(offset)
    str_date = dt.strftime("%Y_%m_%d_%H%M")
    fn = fn_main + '_' + str_date + '.xlsx'
    logger.info(fn + ' save - start ...')
    if e is None or (e <0):
        e = df.shape[0]
    if columns is None:
        df[b:e].to_excel(os.path.join(path_to_save, fn), index = index)
    else:
        df[b:e].to_excel(os.path.join(path_to_save, fn), index = index, columns = columns)
    logger.info(fn + ' saved to ' + path_to_save)
    hfs = get_humanize_filesize(path_to_save, fn)
    logger.info("Size: " + str(hfs))
    return fn

def save_df_lst_to_excel(df_lst, sheet_names_lst, save_path, fn):
    # fn = model + '.xlsx'
    offset = datetime.timezone(datetime.timedelta(hours=3))
    dt = datetime.datetime.now(offset)
    str_date = dt.strftime("%Y_%m_%d_%H%M")
    fn_date = fn.replace('.xlsx','')  + '_' + str_date + '.xlsx'
    
    # with pd.ExcelWriter(os.path.join(path_tkbd_processed, fn_date )) as writer:  
    with pd.ExcelWriter(os.path.join(save_path, fn_date )) as writer:  
        
        for i, df in enumerate(df_lst):
            df.to_excel(writer, sheet_name = sheet_names_lst[i], index=False)
    return fn_date    



def get_humanize_filesize(path, fn):
    human_file_size = None
    try:
        fn_full = os.path.join(path, fn)
    except Exception as err:
        print(err)
        return human_file_size
    if os.path.exists(fn_full):
        file_size = os.path.os.path.getsize(fn_full)
        human_file_size = humanize.naturalsize(file_size)
    return human_file_size
    
def restore_df_from_pickle(path_files, fn_pickle):

    if fn_pickle is None:
        logger.error('Restore pickle from ' + path_files + ' failed!')
        sys.exit(2)
    if os.path.exists(os.path.join(path_files, fn_pickle)):
        df = pd.read_pickle(os.path.join(path_files, fn_pickle))
        # logger.info('Restore ' + re.sub(path_files, '', fn_pickle_СЃ) + ' done!')
        logger.info('Restore ' + fn_pickle + ' done!')
        logger.info('Shape: ' + str(df.shape))
    else:
        # logger.error('Restore ' + re.sub(path_files, '', fn_pickle_СЃ) + ' from ' + path_files + ' failed!')
        logger.error('Restore ' + fn_pickle + ' from ' + path_files + ' failed!')
    return df    



def upload_files_services(supp_dict_dir = '/content/data/supp_dict'):
    base_url = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?'
    # public_key = link #'https://yadi.sk/d/UJ8VMK2Y6bJH7A'  # Сюда вписываете вашу ссылку
    links = [('Коды МГФОМС и 804н.xlsx', 'https://disk.yandex.ru/i/lX1fVnK1J7_hfg', ('МГФОМС', '804н')),
    ('serv_name_embeddings.pk1', 'https://disk.yandex.ru/d/8UTwZg5jKOhxXQ'),
    # ('НВМИ_РМ.xls', 'https://disk.yandex.ru/i/_RotfMJ_cSfeOw', 'Sheet1'),
    # ('МНН.xlsx', 'https://disk.yandex.ru/i/0rMKBimIKbS7ig', 'Sheet1'),
    # ('df_mi_national_release_20230201_2023_02_06_1013.zip', 'https://disk.yandex.ru/d/pfgyT_zmcYrHBw' ),
    # ('df_mi_org_gos_release_20230129_2023_02_07_1331.zip', 'https://disk.yandex.ru/d/Zh-5-FG4uJyLQg' ),
    # ('Специальность (унифицированный).xlsx', 'https://disk.yandex.ru/i/au5M0xyVDW2mtQ', None),
    ]

    # Получаем загрузочную ссылку
    for link_t in links:
        final_url = base_url + urlencode(dict(public_key=link_t[1]))
        response = requests.get(final_url)
        download_url = response.json()['href']

        # Загружаем файл и сохраняем его
        download_response = requests.get(download_url)
        # with open('downloaded_file.txt', 'wb') as f:   # Здесь укажите нужный путь к файлу
        with open(os.path.join(supp_dict_dir, link_t[0]), 'wb') as f:   # Здесь укажите нужный путь к файлу
            f.write(download_response.content)
            logger.info(f"File '{link_t[0]}' uploaded!")
            if link_t[0].split('.')[-1] == 'zip':
                fn_unzip = unzip_file(os.path.join(supp_dict_dir, link_t[0]), '', supp_dict_dir)
                logger.info(f"File '{fn_unzip}' upzipped!")


def load_check_dictionaries_services(path_supp_dicts):
    # global df_services_MGFOMS, df_services_804n, df_RM, df_MNN, df_mi_org_gos, df_mi_national
    # if not os.path.exists(supp_dict_dir):
    #     os.path.mkdir(supp_dict_dir)

    fn = 'Коды МГФОМС.xlsx'
    fn = 'Коды МГФОМС и 804н.xlsx'
    sheet_name = 'МГФОМС'
    df_services_MGFOMS = pd.read_excel(os.path.join(path_supp_dicts, fn), sheet_name = sheet_name)
    df_services_MGFOMS.rename (columns = {'COD': 'code', 'NAME': 'name'}, inplace=True)
    df_services_MGFOMS['code'] = df_services_MGFOMS['code'].astype(str)
    # print("df_services_MGFOMS", df_services_MGFOMS.shape, df_services_MGFOMS.columns)
    logger.info(f"Загружен справочник 'Услуги по реестру  МГФОМС': {str(df_services_MGFOMS.shape)}")

    sheet_name = '804н'
    df_services_804n = pd.read_excel(os.path.join(path_supp_dicts, fn), sheet_name = sheet_name, header=1)
    df_services_804n.rename (columns = {'Код услуги': 'code', 'Наименование медицинской услуги': 'name'}, inplace=True)
    # print("df_services_804n", df_services_804n.shape, df_services_804n.columns)
    logger.info(f"Загружен справочник 'Услуги по приказу 804н': {str(df_services_804n.shape)}")

    fn_pickle = 'serv_name_embeddings.pk1'
    serv_name_embeddings = restore_df_from_pickle(path_supp_dicts, fn_pickle) 
    
    return df_services_MGFOMS, df_services_804n, serv_name_embeddings
# df_services_MGFOMS, df_services_804n, serv_name_embeddings = load_check_dictionaries_services(path_supp_dicts)

def np_unique_nan(lst: np.array, debug = False)->np.array: # a la version 2.4
    lst_unique = None
    if lst is None or (((type(lst)==float) or (type(lst)==np.float64)) and np.isnan(lst)):
        # if debug: print('np_unique_nan:','lst is None or (((type(lst)==float) or (type(lst)==np.float64)) and math.isnan(lst))')
        lst_unique = lst
    else:
        data_types_set = list(set([type(i) for i in lst]))
        if debug: print('np_unique_nan:', 'lst:', lst, 'data_types_set:', data_types_set)
        if ((type(lst)==list) or (type(lst)==np.ndarray)):
            if debug: print('np_unique_nan:','if ((type(lst)==list) or (type(lst)==np.ndarray)):')
            if len(data_types_set) > 1: # несколько типов данных
                if list not in data_types_set and dict not in data_types_set and tuple not in data_types_set and type(None) not in data_types_set:
                    lst_unique = np.array(list(set(lst)), dtype=object)
                else:
                    lst_unique = lst
            elif len(data_types_set) == 1:
                if debug: print("np_unique_nan: elif len(data_types_set) == 1:")
                if list in data_types_set:
                    lst_unique = np.unique(np.array(lst, dtype=object))
                elif  np.ndarray in data_types_set:
                    # print('elif  np.ndarray in data_types_set :')
                    lst_unique = np.unique(lst.astype(object))
                    # lst_unique = np_unique_nan(lst_unique)
                    lst_unique = np.asarray(lst, dtype = object)
                    # lst_unique = np.unique(lst_unique)
                elif type(None) in data_types_set:
                    # lst_unique = np.array(list(set(lst)))
                    lst_unique = np.array(list(set(list(lst))))
                elif dict in  data_types_set:
                    lst_unique = lst
                    # np.unique(lst)
                elif type(lst) == np.ndarray:
                    if debug: print("np_unique_nan: type(lst) == np.ndarray")
                    if (lst.dtype.kind == 'f') or  (lst.dtype == np.float64) or  (float in data_types_set):
                        if debug: print("np_unique_nan: (lst.dtype.kind == 'f')")
                        lst_unique = np.unique(lst.astype(float))
                        # if debug: print("np_unique_nan: lst_unique predfinal:", lst_unique)
                        # lst_unique = np.array(list(set(list(lst))))
                        # if debug: print("np_unique_nan: lst_unique predfinal v2:", lst_unique)
                        # if np.isnan(lst).all():
                        #     lst_unique = np.nan
                        #     if debug: print("np_unique_nan: lst_unique predfinal v3:", lst_unique)
                    elif (lst.dtype.kind == 'S') :
                        if debug: print("np_unique_nan: lst.dtype == string")
                        lst_unique = np.array(list(set(list(lst))))
                        if debug: print(f"np_unique_nan: lst_unique 0: {lst_unique}")
                    elif lst.dtype == object:
                        if debug: print("np_unique_nan: lst.dtype == object")
                        if (type(lst[0])==str) or (type(lst[0])==np.str_) :
                            try:
                                lst_unique = np.unique(lst)
                            except Exception as err:
                                lst_unique = np.array(list(set(list(lst))))
                        else:
                            lst_unique = np.array(list(set(list(lst))))
                        if debug: print(f"np_unique_nan: lst_unique 0: {lst_unique}")
                    else:
                        if debug: print("np_unique_nan: else 0")
                        lst_unique = np.unique(lst)
                else:
                    if debug: print('np_unique_nan:','else i...')
                    lst_unique = np.array(list(set(lst)))
                    
            elif len(data_types_set) == 0:
                lst_unique = None
            else:
                # print('else')
                lst_unique = np.array(list(set(lst)))
        else: # другой тип данных
            if debug: print('np_unique_nan:','другой тип данных')
            # lst_unique = np.unique(np.array(list(set(lst)),dtype=object))
            # lst_unique = np.unique(np.array(list(set(lst)))) # Исходим из того что все елеменыт спсика одного типа
            lst_unique = lst
    if type(lst_unique) == np.ndarray:
        if debug: print('np_unique_nan: final: ', "if type(lst_unique) == np.ndarray")
        if lst_unique.shape[0]==1: 
            if debug: print('np_unique_nan: final: ', "lst_unique.shape[0]==1")
            lst_unique = lst_unique[0]
            if debug: print(f"np_unique_nan: final after: lst_unique: {lst_unique}")
            if (type(lst_unique) == np.ndarray) and (lst_unique.shape[0]==1):  # двойная вложенность
                if debug: print('np_unique_nan: final: ', 'one more', "lst_unique.shape[0]==1")
                lst_unique = lst_unique[0]
        elif lst_unique.shape[0]==0: lst_unique = None
    if debug: print(f"np_unique_nan: return: lst_unique: {lst_unique}")
    if debug: print(f"np_unique_nan: return: type(lst_unique): {type(lst_unique)}")
    return lst_unique

