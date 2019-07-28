#!/usr/bin/env python
"""
cjr_queries - Class to query job progress database.
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
# Purpose: class to record job progress, this class is main interface to the library.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 28/07/2019
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import datetime
from cjrlib.cjr_db_connection import CJRDBConnection, CJRJobName, CJRTaskInfo, task_to_dict


def query_job_names():
    """
    A function to retrieve a list of job names within the database.

    :return: list of strings.

    """
    cjrdb_conn = CJRDBConnection()
    if cjrdb_conn is None:
        raise Exception("Could not create the connection object...")
    cjrdb_conn.create_db_tables()

    db_ses_obj = cjrdb_conn.get_db_session()

    job_names = list()
    qury_rslt = db_ses_obj.query(CJRJobName).all()
    if qury_rslt is not None:
        for job_name_rcd in qury_rslt:
            job_names.append(job_name_rcd.JobName)
    db_ses_obj.close()

    return job_names


def get_all_tasks(job_name, version):
    """
    A function which retrieves all the tasks associated with a job and version

    :param job_name: a string for the name of the job
    :param version: an integer for the version of the task.

    :return: returns a dictionary of the tasks
    """
    cjrdb_conn = CJRDBConnection()
    if cjrdb_conn is None:
        raise Exception("Could not create the connection object...")
    cjrdb_conn.create_db_tables()

    db_ses_obj = cjrdb_conn.get_db_session()

    task_lst = list()
    qury_rslt = db_ses_obj.query(CJRTaskInfo).filter(CJRTaskInfo.JobName == job_name).\
                                              filter(CJRTaskInfo.Version == version).all()
    if qury_rslt is not None:
        for task_rcd in qury_rslt:
            task_lst.append(task_to_dict(task_rcd))
    db_ses_obj.close()

    return task_lst


def get_uncompleted_tasks(job_name, version):
    """
    A function which retrieves the uncompleted tasks associated with a job and version

    :param job_name: a string for the name of the job
    :param version: an integer for the version of the task.

    :return: returns a dictionary of the tasks
    """
    cjrdb_conn = CJRDBConnection()
    if cjrdb_conn is None:
        raise Exception("Could not create the connection object...")
    cjrdb_conn.create_db_tables()

    db_ses_obj = cjrdb_conn.get_db_session()

    task_lst = list()
    qury_rslt = db_ses_obj.query(CJRTaskInfo).filter(CJRTaskInfo.JobName == job_name).\
                                              filter(CJRTaskInfo.Version == version).\
                                              filter(CJRTaskInfo.TaskCompleted == False).all()
    if qury_rslt is not None:
        for task_rcd in qury_rslt:
            task_lst.append(task_to_dict(task_rcd))
    db_ses_obj.close()

    return task_lst


def get_tasks(job_name, task_id):
    """
    A function which retrieves the uncompleted tasks associated with a job and version

    :param job_name: a string for the name of the job
    :param task_id: n string for the task ID.

    :return: returns a dictionary of the tasks
    """
    cjrdb_conn = CJRDBConnection()
    if cjrdb_conn is None:
        raise Exception("Could not create the connection object...")
    cjrdb_conn.create_db_tables()

    db_ses_obj = cjrdb_conn.get_db_session()

    task_lst = list()
    qury_rslt = db_ses_obj.query(CJRTaskInfo).filter(CJRTaskInfo.JobName == job_name).\
                                              filter(CJRTaskInfo.TaskID == task_id).all()
    if qury_rslt is not None:
        for task_rcd in qury_rslt:
            task_lst.append(task_to_dict(task_rcd))
    db_ses_obj.close()

    return task_lst
