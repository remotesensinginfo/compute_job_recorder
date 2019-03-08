#!/usr/bin/env python
"""
cjr_db_connection - DB Connection class. Singleton class to reuse database connection.
"""
# This file is part of 'compute_job_recorder'
# A library for recording compute job progress.
#
# Copyright 2019 Pete Bunting
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Purpose: DB Connection class. Singleton class to reuse database
#          connection, where possible.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 08/02/2019
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import configparser.ConfigParser
import psycopg2
import os
import os.path
import logging

logger = logging.getLogger(__name__)

def parse_db_config(filename='database.ini', section='postgresql'):
    """
    Parse a database configuration file. The configuration file
    should have the following format:

    [postgresql]
    host=localhost
    port=5432
    dbname=cjr_records
    username=postgres
    password=postgres

    :param filename: file name and path to the configuration file.
    :param section: name of the section within the configure file.
    :return: return dict with database connection info
    """
    # create a parser
    parser = configparser.ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db_info = {}
    db_info['db_conn_info'] = dict()
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db_info['db_conn_info'][param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db_info


class CJRDBConnection:
    """
    Database connection class
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = object.__new__(cls)

            try:
                ini_db_file = os.environ['CJR_DB_CONFIG']
            except Exception:
                raise Exception("""Environmental variable CJR_DB_CONFIG was not defined and therefore
                                   the database connection information could not be found.""")

            try:
                ini_db_file_section = os.environ['CJR_DB_CONFIG_SECTION']
            except Exception:
                ini_db_file_section = 'postgresql'

            if not os.path.exists(ini_db_file):
                raise Exception("Database connection ini file could not be found '{}'".format(ini_db_file))

            db_config_info = parse_db_config(ini_db_file, ini_db_file_section)

            db_config = dict()
            if 'host' in db_config_info['db_conn_info']:
                db_config['host'] = db_config_info['db_conn_info']['host']
            else:
                raise Exception("The database 'host' was not given.")

            if 'port' in db_config_info['db_conn_info']:
                db_config['port'] = int(db_config_info['db_conn_info']['port'])
            else:
                db_config['port'] = 5432

            if 'dbname' in db_config_info['db_conn_info']:
                db_config['dbname'] = db_config_info['db_conn_info']['dbname']
            else:
                raise Exception("The database name ('dbnmae') was not given.")

            if 'username' in db_config_info['db_conn_info']:
                db_config['user'] = db_config_info['db_conn_info']['username']
            else:
                raise Exception("The database 'username' was not given.")

            if 'password' in db_config_info['db_conn_info']:
                db_config['password'] = db_config_info['db_conn_info']['password']
            else:
                raise Exception("The database 'password' was not given.")

            try:
                print('Connecting to PostgreSQL database...')
                connection = CJRDBConnection._instance.connection = psycopg2.connect(**db_config)
                cursor = CJRDBConnection._instance.cursor = connection.cursor()
                cursor.execute('SELECT VERSION()')
                db_version = cursor.fetchone()

            except Exception as error:
                print('Error: connection not established {}'.format(error))
                CJRDBConnection._instance = None

            else:
                print('connection established\n{}'.format(db_version[0]))

        return cls._instance

    def __init__(self):
        self.connection = self._instance.connection
        self.cursor = self._instance.cursor

    def query(self, query):
        try:
            result = self.cursor.execute(query)
        except Exception as error:
            print('error execting query "{}", error: {}'.format(query, error))
            return None
        else:
            return result

    def __del__(self):
        self.connection.close()
        self.cursor.close()