from urllib.error import HTTPError, URLError
from urllib.request import urlopen
from scratchclient import ScratchSession
import consts_drscratch as consts
import json
import traceback
import argparse
import os
import sys
import time
import threading
from datetime import datetime 
from zipfile import ZipFile, BadZipfile
import uuid

SUMMARY_DIR = os.path.join(os.path.dirname(__file__), "summaries")
PROJECTS_SUCCESS = "projects_downloaded"
PROJECTS_FAILED = "projects_failed"
PROJECTS_DOWNLOADED = 0
PROJECTS_NO_DOWNLOADED = 0
SESSION = str(datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))

print("""
                                                                                                                                                 
   ____             __      __     ___                  __             __       
  / __/__________ _/ /_____/ /    / _ \___ _    _____  / /__  ___ ____/ /__ ____
 _\ \/ __/ __/ _ `/ __/ __/ _ \  / // / _ \ |/|/ / _ \/ / _ \/ _ `/ _  / -_) __/
/___/\__/_/  \_,_/\__/\__/_//_/ /____/\___/__,__/_//_/_/\___/\_,_/\_,_/\__/_/   
                                                                                
        Author: Daniel Escobar - This project is under MIT License

    If you have any question please contact \033[34mdaniesmor@gsyc.urjc.es                   
                                                                                                                                                                                                                                                                           
""")


parser = argparse.ArgumentParser(description='An scratch project downloader')

parser.add_argument('--identifier', type=int, help='An scratch project ID', required=True)
parser.add_argument('--amount', type=int, help='An integer ammount of projects', required=True)

args = parser.parse_args()

print(f"We are going to download {args.amount} projects starting from project {args.identifier}.")


def send_request_getsb3(id_project):
    """
    Send request to getsb3 app
    """

    file_url = '{}{}'.format(id_project, '.sb3')
    path_project = os.path.join(os.path.dirname(__file__))
    path_json_file_temporary = download_scratch_project_from_servers(path_project, id_project)
    save_projectsb3(path_json_file_temporary, id_project)
    

def download_scratch_project_from_servers(path_project, id_project):
    try:
        scratch_project_inf = ScratchSession().get_project(id_project)
        url_json_scratch = "{}/{}?token={}".format(consts.URL_SCRATCH_SERVER, id_project, scratch_project_inf.project_token)
        path_utemp = os.path.join(os.path.dirname(__file__),"utemp")
        if not os.path.exists(path_utemp):
            os.mkdir(path_utemp)
        path_json_file = os.path.join(path_utemp, str(id_project) + '_new_project.json')
    except KeyError:
        raise KeyError

    try:
        response_from_scratch = urlopen(url_json_scratch)
    except HTTPError:
        url_json_scratch = "{}/{}".format(consts.URL_GETSB3, id_project)
        response_from_scratch = urlopen(url_json_scratch)
        path_json_file = os.path.join(os.path.dirname(__file__),"utemp",str(id_project) + '_old_project.json')
    except URLError:
        traceback.print_exc()
    except:
        traceback.print_exc()

    try:
        json_string_format = response_from_scratch.read()
        json_data = json.loads(json_string_format)
        resulting_file = open(path_json_file, 'wb')
        resulting_file.write(json_string_format)
        resulting_file.close()
    except IOError as e:
        pass

    return path_json_file


def save_projectsb3(path_file_temporary, id_project):
    downloads_dir = os.path.join(os.path.dirname(__file__), "downloads")
    if not os.path.isdir(downloads_dir):
        os.mkdir(downloads_dir)
    dir_zips = os.path.join(downloads_dir, SESSION)
    if not os.path.isdir(dir_zips):
        os.mkdir(dir_zips)

    unique_file_name_for_saving = os.path.join(dir_zips, str(id_project) + ".sb3")

    dir_utemp = path_file_temporary.split(str(id_project))[0].encode('utf-8')
    path_project = os.path.dirname(os.path.dirname(__file__))

    if '_new_project.json' in path_file_temporary:
        ext_project = '_new_project.json'
    else:
        ext_project = '_old_project.json'

    temporary_file_name = str(id_project) + ext_project

    os.chdir(dir_utemp)

    try:
        if os.path.exists(temporary_file_name):
            os.rename(temporary_file_name, 'project.json')
            with ZipFile(unique_file_name_for_saving, 'w') as myzip:
                myzip.write('project.json')
            os.remove('project.json')
        else:
            raise FileNotFoundError(f"Temporal file {temporary_file_name} does not exists in {dir_utemp}")
    finally:
        os.chdir(path_project)


def spinner(stop_event, id_project):
    sys.stdout.write(f"Downloading project {id_project}... ")

def create_summary():
    summaries_dir = os.path.join(os.path.dirname(__file__), SUMMARY_DIR)
    if not os.path.isdir(summaries_dir):
        os.mkdir(summaries_dir)
    os.mkdir(os.path.join(SUMMARY_DIR, SESSION))
    with open(os.path.join(SUMMARY_DIR, SESSION, PROJECTS_SUCCESS), "w") as downloaded:
        pass        
    with open(os.path.join(SUMMARY_DIR, SESSION, PROJECTS_FAILED), "w") as failed:
        pass
        

def log_successful(project_id, downloaded):
    if downloaded:
        summary_file = os.path.join(SUMMARY_DIR, SESSION, PROJECTS_SUCCESS)
    else:
        summary_file = os.path.join(SUMMARY_DIR, SESSION, PROJECTS_FAILED)
    with open(summary_file, 'a') as summary:
        summary.write(str(project_id) + "\n")


start_time = time.time()
create_summary()
start_id = args.identifier
while PROJECTS_DOWNLOADED < args.amount:
    try:
        stop_event = threading.Event()
        spinner_thread = threading.Thread(target=spinner, args=(stop_event,start_id,))
        spinner_thread.start()
        send_request_getsb3(start_id)
        
        stop_event.set()
        print("\033[92m" + f"The project {start_id} has been successfully downloaded.")
        PROJECTS_DOWNLOADED += 1
        log_successful(start_id, True)
    except KeyError:
        PROJECTS_NO_DOWNLOADED += 1
        print("\033[91m" + f"The project {start_id} does not exists.")
        log_successful(start_id, False)
    start_id += 1
end_time = time.time()
elapsed_time = end_time - start_time


print(F""" 
###############################################################
##
##   SESSION {SESSION} SUMMARY
##   - {PROJECTS_DOWNLOADED} projects downloaded.
##   - {PROJECTS_NO_DOWNLOADED} projects failed.
##   - It tooks {elapsed_time} seconds.
##      
##   Projects downloaded are located in downloades dir.
##   For more info see summaries directory. Thanks for use.
##
#################################################################
""")