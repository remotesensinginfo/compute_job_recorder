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

import sqlalchemy
import sqlalchemy.orm
from sqlalchemy.ext.declarative import declarative_base
import os
import os.path
import datetime

Base = declarative_base()


class CJRJobName(Base):
    __tablename__ = "CJRJobName"
    JobName = sqlalchemy.Column(sqlalchemy.String, primary_key=True)


class CJRTaskInfo(Base):
    __tablename__ = "CJRTaskInfo"
    TaskID = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
    JobName = sqlalchemy.Column(sqlalchemy.String, sqlalchemy.ForeignKey("CJRJobName.JobName"), primary_key=True)
    Version = sqlalchemy.Column(sqlalchemy.INTEGER, primary_key=True)
    StartTime = sqlalchemy.Column(sqlalchemy.DateTime)
    EndTime = sqlalchemy.Column(sqlalchemy.DateTime)
    TaskParams = sqlalchemy.Column(sqlalchemy.JSON)
    TaskUpdates = sqlalchemy.Column(sqlalchemy.JSON)
    TaskEndInfo = sqlalchemy.Column(sqlalchemy.JSON)
    TaskCompleted = sqlalchemy.Column(sqlalchemy.Boolean, default=False)


def task_to_dict(task_rcd):
    """
    A function to convert a CJRTaskInfo record to a dictionary

    :param task_rcd: record from the CJRTaskInfo table.

    :return: returns a dictionary

    """
    task_dict = dict()
    task_dict['task_id'] = task_rcd.TaskID
    task_dict['job_name'] = task_rcd.JobName
    task_dict['version'] = task_rcd.Version
    start_time_str = task_rcd.StartTime
    if (start_time_str is None) or (start_time_str == ""):
        task_dict['start_time'] = None
    else:
        task_dict['start_time'] = datetime.datetime.fromisoformat(start_time_str)
    end_time_str = task_rcd.EndTime
    if (end_time_str is None) or (end_time_str == ""):
        task_dict['end_time'] = None
    else:
        task_dict['end_time'] = datetime.datetime.fromisoformat(end_time_str)
    task_dict['params'] = task_rcd.TaskParams
    task_dict['update_info'] = task_rcd.TaskUpdates
    task_dict['end_info'] = task_rcd.TaskEndInfo
    task_dict['completed'] = task_rcd.TaskCompleted


class CJRDBConnection:
    """
    Database connection class
    """
    _instance = None
    print_progress = False

    def __new__(cls):
        """
        Function which creates the DB connection object as a singularity model.
        """
        if cls._instance is None:
            cls._instance = object.__new__(cls)

            try:
                cls._instance.cjr_db_file = os.environ['CJR_DB_FILE']
            except Exception:
                raise Exception("""Environmental variable CJR_DB_FILE was not defined and therefore
                                   the database file has not been specified.""")

            try:
                cls._instance.db_engine = sqlalchemy.create_engine(cls._instance.cjr_db_file)
            except Exception as error:
                print('Error: connection not established {}'.format(error))
                cls._instance = None

        return cls._instance

    def __init__(self):
        self.db_engine = self._instance.db_engine
        self.cjr_db_file = self._instance.cjr_db_file

    def set_print_progress(self, print_progress=False):
        """
        Function which defines the parameter print_progress. If True then progress information will be printed to
        the console. If False then it will not be printed (Default).

        :param print_progress: boolean on whether progress is printed to the console or not.

        """
        self.print_progress = print_progress

    def create_db_tables(self):
        """
        A function which checks whether Tables exist and if they don't
        then create the DB Tables.
        """
        if self.print_progress:
            print("Check whether the tables were already present.")
        if not self.db_engine.dialect.has_table(self.db_engine, "CJRJobName"):
            if self.print_progress:
                print("Drop usage table if within the existing database.")
            Base.metadata.drop_all(self.db_engine)

            if self.print_progress:
                print("Creating Usage Database.")
            Base.metadata.bind = self.db_engine
            Base.metadata.create_all()

    def get_db_session(self):
        """
        Get a database session object.

        :return: return an sqlalchemy session object.
        """
        session = sqlalchemy.orm.sessionmaker(bind=self.db_engine)
        ses = session()
        return ses

    #def __del__(self):
    #    if self.db_engine is not None:
    #        self.db_engine.close()
