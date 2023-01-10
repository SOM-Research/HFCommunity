#!/usr/bin/env python
# -*- coding: utf-8 -*-

import getopt
import subprocess
import sys
import os
import sqlite3 
import huggingface_hub
import requests
import time
from pydriller import Repository
from git.exc import GitCommandError
import huggingface_hub.utils._errors as hf_errors
import mysql.connector
import json
from random import sample
from time import sleep
from huggingface_hub import HfApi
import itertools
from cleantext import clean


ACCESS_TOKEN = ''

# ERROR PRINTING
def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def create_connection_mysql():
      """ 
      Create a database connection to a MySQL/MariaDB database.
      Database configuration file must be located in the path as a JSON file called db.config with the following structure:
      {
            "host" : HOST,
            "port" : PORT,
            "user" : USER,
            "pass" : PASS,
            "database" : SCHEMANAME
      }
      """

      conn = None
      if os.path.exists('db.config'):
            config = json.load(open('db.config'))
      else:
            eprint('db.config file not found!')
            sys.exit(1)
      
      try :
            conn = mysql.connector.connect(user=config['user'], password=config['pass'], host=config['host'], port=config['port'], database=config['database'])  
      except:
            eprint("Database connection failed")
            sys.exit(1)
      return conn


def create_schema_mysql(cursor):
      """ create the database schema """

      print("Deleting tables...")
      
      cursor.execute('''DROP TABLE IF EXISTS tag''')
      cursor.execute('''DROP TABLE IF EXISTS author''')
      cursor.execute('''DROP TABLE IF EXISTS file''')
      cursor.execute('''DROP TABLE IF EXISTS files_in_repo''')
      cursor.execute('''DROP TABLE IF EXISTS tags_in_repo''')
      cursor.execute('''DROP TABLE IF EXISTS discussion''')
      cursor.execute('''DROP TABLE IF EXISTS model''')
      cursor.execute('''DROP TABLE IF EXISTS dataset''')
      cursor.execute('''DROP TABLE IF EXISTS space''')
      cursor.execute('''DROP TABLE IF EXISTS models_in_spaces''')
      cursor.execute('''DROP TABLE IF EXISTS commits''')
      cursor.execute('''DROP TABLE IF EXISTS files_in_commit''')
      cursor.execute('''DROP TABLE IF EXISTS discussion_event''')

      print("Creating tables...")

      cursor.execute('''
            CREATE TABLE IF NOT EXISTS tag
            (name VARCHAR(256) PRIMARY KEY)
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS author
            (name VARCHAR(256) PRIMARY KEY, avatar_url TEXT, is_pro INTEGER, fullname TEXT, type VARCHAR(64), source VARCHAR(256))
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS file
            (id INTEGER PRIMARY KEY AUTO_INCREMENT, filename VARCHAR(500), repo_id VARCHAR(256), UNIQUE(filename, repo_id))
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS tags_in_repo
            (tag_name VARCHAR(256), repo_id VARCHAR(256), PRIMARY KEY(tag_name, repo_id))
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS discussion
            (num INTEGER, repo_id VARCHAR(256), author TEXT, title TEXT, status TEXT, created_at TEXT, is_pull_request INTEGER, PRIMARY KEY(num, repo_id))
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS model
            (id VARCHAR(256) PRIMARY KEY, name TEXT, model_id TEXT, author TEXT, sha TEXT, last_modified TEXT, private INTEGER, pipeline_tag TEXT, downloads INTEGER, library_name TEXT, likes INTEGER, config LONGTEXT, card_data LONGTEXT, gated INTEGER)
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS dataset
            (id VARCHAR(256) PRIMARY KEY, name TEXT, author TEXT, sha TEXT, last_modified TEXT, private INTEGER, gated INTEGER, citation TEXT, description TEXT, downloads INTEGER, likes INTEGER, card_data LONGTEXT, paperswithcode_id TEXT)
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS space
            (id VARCHAR(256) PRIMARY KEY, name TEXT, author TEXT, sha TEXT, last_modified TEXT, private INTEGER, card_data LONGTEXT, gated INTEGER)
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS models_in_spaces
            (model_id VARCHAR(256), space_id VARCHAR(256), PRIMARY KEY(model_id, space_id))
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS commits
            (sha VARCHAR(256) PRIMARY KEY, timestamp TEXT, message TEXT, author TEXT)
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS files_in_commit
            (sha VARCHAR(256), file_id INTEGER, PRIMARY KEY(sha, file_id))
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS discussion_event
            (id VARCHAR(256) PRIMARY KEY, repo_id VARCHAR(256), discussion_num INTEGER,  type VARCHAR(64), created_at TEXT, author TEXT, content TEXT, edited INTEGER, hidden INTEGER, new_status TEXT, summary TEXT, sha TEXT, old_title TEXT, new_title TEXT, full_data LONGTEXT)
            ''')

      print("Tables created!")


# UTILITY FUNCTIONS
def check_attribute(dict, attr):
      """ function to check whether an attribute is in a dictionary """

      attribute = ""
      try:
            attribute = dict.__getattribute(attr)
      except:
            attribute = None

      return attribute


# POPULATE FUNCTIONS
def populate_tags(cursor, tags, repo_name, type):
      """ importation of tag information """
  
      for tag in tags:
            
            cursor.execute('''
            INSERT IGNORE INTO tag (name) VALUES (%s)
            ''', [tag])
            cursor.execute('''
            INSERT IGNORE INTO tags_in_repo (tag_name, repo_id) VALUES (%s, %s)
            ''', (tag, type + '/' + repo_name))


def populate_files(cursor, files, repo_name, type):
      """ importation of file information (just API) """
 
      for file in files:
            if type == "models" or type == "spaces":
                  filename = file.rfilename
            elif type == "datasets":
                  filename = file

            cursor.execute('''
            INSERT IGNORE INTO file (filename, repo_id) VALUES (%s, %s)
            ''', (filename, type + '/' + repo_name))


def populate_dataset_files(cursor, dataset_id, api):
      n = 0
      while n < 5:
            try:
                  populate_files(cursor, api.list_repo_files(repo_id=dataset_id, repo_type="dataset", use_auth_token=ACCESS_TOKEN), dataset_id, "datasets")
                  n = 5
            except hf_errors.RepositoryNotFoundError as e:
                  eprint("---------DATASET FILES ERROR---------")
                  eprint("ERROR: Repository Not Found (Repo: (dataset) " + dataset_id + "). Printing exception...")
                  eprint(e)
                  n = 5
            except requests.exception.HTTPError as e:
                  eprint("---------DATASET FILES ERROR---------")
                  eprint("ERROR: HTTPError on repo (dataset): " + dataset_id + ". Exception:\n" + e)
                  eprint("Retrying...")
                  sleep(10)
                  n = n + 1


def populate_discussions(cursor, api, repo_name, type):
      """ Retrieve discussion from a repo and populate discussion table """

      try:
            for discussion in api.get_repo_discussions(repo_id=repo_name, repo_type=type, token=ACCESS_TOKEN):
                  
                  details = api.get_discussion_details(repo_id=repo_name, discussion_num=discussion.num, repo_type=type, token=ACCESS_TOKEN)

                  author_name = details.__dict__.get("author")
                  if author_name == 'deleted':
                        author_name = ""

                  cursor.execute('''
                                    INSERT IGNORE INTO discussion (num, repo_id, author, title, status, created_at, is_pull_request) VALUES (%s, %s, %s, %s, %s, %s, %s)
                                    ''', (details.__dict__.get("num"), type + 's/' + repo_name, author_name, details.__dict__.get("title"), details.__dict__.get("status"), details.__dict__.get("created_at"), details.__dict__.get("is_pull_request")))

                  for event in details.events:
                        author_name = event.__dict__.get("author")
                        if author_name == 'deleted':
                              author_name = ""

                        if event.type == "comment":
                              cursor.execute('''
                                    INSERT IGNORE INTO discussion_event (id, repo_id, discussion_num, type, created_at, author, content, edited, hidden, full_data) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    ''', (event.__dict__.get("id"), type + 's/' + repo_name, discussion.num,  event.__dict__.get("type"), event.__dict__.get("created_at"), author_name, event.__dict__.get("content"), event.__dict__.get("edited"), event.__dict__.get("hidden"), str(event.__dict__.get("_event"))))
                        elif event.type == "status-change":
                              cursor.execute('''
                                    INSERT IGNORE INTO discussion_event (id, repo_id, discussion_num, type, created_at, author, new_status, full_data) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                    ''', (event.__dict__.get("id"), type + 's/' + repo_name, discussion.num, event.__dict__.get("type"), event.__dict__.get("created_at"), author_name, event.__dict__.get("new_status"), str(event.__dict__.get("_event"))))
                        elif event.type == "commit":
                              cursor.execute('''
                                    INSERT IGNORE INTO discussion_event (id, repo_id, discussion_num, type, created_at, author,summary, sha, full_data) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    ''', (event.__dict__.get("id"), type + 's/' + repo_name, discussion.num, event.__dict__.get("type"), event.__dict__.get("created_at"), author_name, event.__dict__.get("summary"), event.__dict__.get("oid"), str(event.__dict__.get("_event"))))
                        elif event.type == "title-change":
                              cursor.execute('''
                                    INSERT IGNORE INTO discussion_event (id, repo_id, discussion_num, type, created_at, author, old_title, new_title, full_data) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    ''', (event.__dict__.get("id"), type + 's/' + repo_name, discussion.num, event.__dict__.get("type"), event.__dict__.get("created_at"), author_name, event.__dict__.get("old_title"), event.__dict__.get("new_title"), str(event.__dict__.get("_event"))))
                        
                        author = event.__dict__.get('_event').get('author')

                        if author is not None:
                              cursor.execute('''
                                    REPLACE INTO author (username, avatar_url, is_pro, fullname, type, source) VALUES (%s, %s, %s, %s, %s, %s)
                                    ''', (author.get("name"), author.get("avatarUrl"), author.get("isPro"), author.get("fullname"), author.get("type"), "hf"))
      except Exception as e:
            eprint("---------DISCUSSIONS ERROR----------")
            eprint("Printing exception:")
            eprint(e)
            eprint("Repo: ", repo_name)



def populate_commits(cursor, conn, repo_id, type):
      """ importation of commit information using PyDriller """

      # As we have to do some SELECTs to the database, we commit all the INSERTs until now
      conn.commit()

      url_prefix = ""
      if type != "models":
            url_prefix = type + '/'
      

      url = 'https://user:password@huggingface.co/' + url_prefix + repo_id

      try:
            subprocess.check_output(["git",  "clone",  "--bare",  url])
      except subprocess.CalledProcessError:
            eprint("HF Repository clone error (repo: " + repo_id + "): authentication error.\n")
            return

      # print(repo_id)
      if '/' in repo_id:
            repo_folder = repo_id.split('/')[1] + '.git'
      else:
            repo_folder = repo_id + '.git'

      # path = os.getcwd() + '\\' + repo_folder # WINDOWS
      path = os.getcwd() + '/' + repo_folder # LINUX
      repo = Repository(path)
      
      
      
      try:
            cursor.execute("SELECT filename, id FROM file WHERE repo_id=%s", [type + '/' + repo_id])
            repo_files = dict(cursor.fetchall())

            for commit in repo.traverse_commits():
                  
                  cursor.execute('''
                  INSERT IGNORE INTO commits (sha, timestamp, message, author) VALUES (%s, %s, %s, %s)
                  ''', (commit.hash, commit.author_date, commit.msg, commit.author.name))
                  cursor.execute('''
                  INSERT IGNORE INTO author (name, source) VALUES (%s, %s)
                  ''', (commit.author.name, "commit"))

                  n = 0

                  # try:
                  for file in commit.modified_files:
                        file_id = repo_files.get(file.filename)
                        if file_id is not None:
                              cursor.execute('''
                              INSERT IGNORE INTO files_in_commit (sha, file_id) VALUES (%s, %s)
                              ''', (commit.hash, file_id))
                        else:
                              n += 1

                  if n > 0:
                        eprint(str(n) + " file(s) of repo (type: " + type + ", sha: ", commit.hash, ") " + repo_id + " not found.")
                  
            
            # os.system("rmdir /q /s " + repo_folder) # WINDOWS
            os.system("rm -rf " + repo_folder) # LINUX
     
      except GitCommandError:
            eprint("\n")
            eprint("PyDriller Repository clone error (repo: " + repo_id + "): \n", sys.exc_info())
            eprint("\n")
            return
      


def populate_models(cursor,conn, api, lower, upper, update):
      """ importation of the full information of models """

      print("Retrieving full information of models...")
      models = api.list_models(full=True, cardData=True, fetch_config=True, use_auth_token=ACCESS_TOKEN)
      print("Info retrieved!")

      # models = sample(models, len(models))
      print("Starting population into database...")

      for model in itertools.islice(models, lower, upper):

            ### This is a workaround I made to not add models when I updated some fields
            
            if (update): #update only models already in the database
                  to_update = False
                  with open('models_id.txt') as f:
                        lines = f.read().splitlines()
                        if ('models/' + model.modelId in lines):
                              to_update = True # It's one of the repos we gathered

                  # with open('commit_repo_id.txt') as f:
                  #       lines = f.read().splitlines()
                  #       if ('models/' + model.modelId in lines):
                  #             to_update = False # We already populated this repo
                  if to_update:
                        populate_files(cursor, model.siblings, model.modelId, "models")
                        populate_commits(cursor, conn, model.modelId, "models") # Just update commits
                        populate_discussions(cursor, api, model.modelId, "model")
            else:

                  model_id = "models/" + model.id
                  config = str(model.__dict__.get("config"))
                  config = clean(config, no_emoji=True)
                  cursor.execute('''
                  INSERT INTO model (id, name, model_id, author, sha, last_modified, pipeline_tag, private, config, card_data, downloads, library_name, likes) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE name = values(name), model_id = values(model_id), author = values(author), sha = values(sha), last_modified= values(last_modified), pipeline_tag = values(pipeline_tag), private = values(private), config = values(config), card_data = values(card_data), library_name = values(library_name), likes = values(likes))
                  ''', (model_id, model.__dict__.get("id"), model.__dict__.get("modelId"), model.__dict__.get("author"), model.__dict__.get("sha"), model.__dict__.get("lastModified"), model.__dict__.get("pipeline_tag"), model.__dict__.get("private"), config, str(model.__dict__.get("cardData")), model.__dict__.get("downloads"), model.__dict__.get("library_name"), model.__dict__.get("likes")))
                  
                  if (model.__dict__.get("author") != None):
                        cursor.execute('''
                        INSERT IGNORE INTO author (name, source) VALUES (%s, %s)
                        ''', (model.author, "hf_owner"))

                  populate_tags(cursor, model.tags, model.modelId, "models")
                  populate_files(cursor, model.siblings, model.modelId, "models")
                  populate_discussions(cursor, api, model.modelId, "model")
                  populate_commits(cursor, conn, model.modelId, "models")


      print("Models populated!")


def populate_datasets(cursor, conn, api, lower, upper, update):
      """ importation of dataset data """

      print("Retrieving information of datasets...")

      datasets = api.list_datasets(full=True, cardData=True, use_auth_token=ACCESS_TOKEN)


      for dataset in itertools.islice(datasets, lower, upper):

            
            if (update): #update only models already in the database
                  to_update = False
                  with open('datasets_id.txt') as f:
                        lines = f.read().splitlines()
                        if ('datasets/' + dataset.id in lines):
                              to_update = True # It's one of the repos we gathered

                  ## AUXILIARY FILES TO SELECT WHICH REPOS WE WANT
                  # with open('commit_repo_id.txt') as f:
                  #       lines = f.read().splitlines()
                  #       if ('datasets/' + dataset.id in lines):
                  #             to_update = False # We already populated this repo
                  
                  # with open('repos_to_do.txt') as f:
                  #       lines = f.read().splitlines()
                  #       if ('datasets/' + dataset.id in lines):
                  #             to_update = False # We will do it another time

                  if to_update:
                        populate_dataset_files(cursor, dataset.id, api)
                        populate_commits(cursor, conn, dataset.id, "datasets") # Just update commits
                        populate_discussions(cursor, api, dataset.id, "dataset")
            
            else: 
                  dataset_id = "datasets/" + dataset.__dict__.get("id")

                  cursor.execute('''
                  INSERT INTO dataset (id, name, author, sha, last_modified, private, description, citation, card_data, gated, downloads, likes, paperswithcode_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE name = values(name), author = values(author), sha = values(sha), last_modified= values(last_modified), private = values(private), description = values(description), citation = values(citation), card_data = values(card_data), gated = values(gated), downloads = values(downloads), likes = values(likes), paperswithcode_id = values(paperswithcode_id)
                  ''', (dataset_id, dataset.__dict__.get("id"), dataset.__dict__.get("author"), dataset.__dict__.get("sha"), dataset.__dict__.get("lastModified"), dataset.__dict__.get("private"), dataset.__dict__.get("description"), dataset.__dict__.get("citation"), str(dataset.__dict__.get("cardData")), dataset.__dict__.get("gated"), dataset.__dict__.get("downloads"), dataset.__dict__.get("likes"), dataset.__dict__.get("paperswithcode_id")))
                  
                  if (dataset.author != None):
                        cursor.execute('''
                        INSERT IGNORE INTO author (name, source) VALUES (%s, %s)
                        ''', (dataset.author, "hf_owner"))
                  
                  

                  # dataset.siblings is always None, but we can use another API endpoint
                  n = 0
                  while n < 5:
                        try:
                              populate_files(cursor, api.list_repo_files(repo_id=dataset.id, repo_type="dataset", use_auth_token=ACCESS_TOKEN), dataset.id, "datasets")
                              n = 5
                        except hf_errors.RepositoryNotFoundError as e:
                              eprint("---------DATASET FILES ERROR---------")
                              eprint("ERROR: Repository Not Found (Repo: (dataset) " + dataset.id + "). Printing exception...")
                              eprint(e)
                              n = 5
                        except requests.exception.HTTPError as e:
                              eprint("---------DATASET FILES ERROR---------")
                              eprint("ERROR: HTTPError on repo (dataset): " + dataset.id + ". Exception:\n" + e)
                              eprint("Retrying...")
                              sleep(10)
                              n = n + 1
                  
                  populate_tags(cursor, dataset.tags, dataset.id, "datasets")
                  populate_discussions(cursor, api, dataset.id, "dataset")
                  populate_commits(cursor, conn, dataset.id, "datasets")

      
      print("Dataset information retrieved!")


def populate_spaces(cursor, conn, api, lower, upper, update):
      """ importation of space data """

      print("Retrieving information of spaces...")
      spaces = api.list_spaces(full=True, use_auth_token=ACCESS_TOKEN) # gated is not retrieved

      
      i = 0
      for space in itertools.islice(spaces, lower, upper):
            i += 1
            if (update): #update only models already in the database
                  to_update = False
                  with open('spaces_id.txt') as f:
                        lines = f.read().splitlines()
                        if ('spaces/' + space.id in lines):
                              to_update = True # It's one of the repos we gathered

                  # with open('commit_repo_id.txt') as f:
                  #       lines = f.read().splitlines()
                  #       if ('spaces/' + space.id in lines):
                  #             to_update = False # We already populated this repo
                  
                  # with open('repos_to_do.txt') as f:
                  #       lines = f.read().splitlines()
                  #       if ('spaces/' + space.id in lines):
                  #             to_update = False # We will do it another time

                  if to_update:
                        populate_files(cursor, space.siblings, space.id, "spaces")
                        populate_commits(cursor, conn, space.id, "spaces") 
                        populate_discussions(cursor, api, space.id, "space")
                        
            else:
                  cursor.execute(''' 
                  INSERT INTO space (id, name, author, sha, last_modified, private, card_data) VALUES (%s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE name = values(name), author = values(author), sha = values(sha), last_modified= values(last_modified), private = values(private), card_data = values(card_data)
                  ''', ("spaces/" + space.__dict__.get("id"), space.__dict__.get("id"), space.__dict__.get("author"), space.__dict__.get("sha"), space.__dict__.get("lastModified"), space.__dict__.get("private"), str(space.__dict__.get("cardData"))))
                  

                  if (space.__dict__.get("author") != None):
                        cursor.execute('''
                        INSERT IGNORE INTO author (name, source) VALUES (%s, %s)
                        ''', (space.author, "hf_owner"))
                  
                  populate_tags(cursor, space.tags, space.id, "spaces")
                  populate_files(cursor, space.siblings, space.id, "spaces")
                  populate_discussions(cursor, api, space.id, "space")
                  populate_commits(cursor, conn, space.id, "spaces")


      print("Spaces information retrieved!")



def main(argv):

      start_time = time.time()

      if len(argv) == 0:
        sys.exit(1)

      try:
            opts, args = getopt.getopt(argv, "t:l:u:o:", [])
      except getopt.GetoptError:
            eprint("Wrong usage. USAGE: python databaseImport.py -t type -l lower_index -u upper_index -o anything(not implemented yet)")
            sys.exit(1)
      
      token_file = open("read_token", "r")
      ACCESS_TOKEN = token_file.readline()
      token_file.close()

      lower = 0
      upper = 1000
      type = ""
      update = False
      for opt, arg in opts:
            if opt in ("-t"):
                  type = arg
            if opt in ("-l"):
                  lower = int(arg)
            if opt in ("-u"):
                  upper = int(arg)
            if opt in ("-o"):
                  update = True

      # conn = create_connection_sqlite(r"hf_database.db") # TODO: change behaviour according to command line options
      conn = create_connection_mysql()
      c = conn.cursor()
      print("connection done")

      c.execute('SET NAMES utf8mb4')
      c.execute("SET CHARACTER SET utf8mb4")
      c.execute("SET character_set_connection=utf8mb4")

      if 'db' in sys.argv:
            create_schema_mysql(c)

      print("schema created")
      api = HfApi()

      if type == "dataset":
            populate_datasets(c, conn, api, lower, upper, update)
      elif type == "space":
            populate_spaces(c, conn, api, lower, upper, update)
      elif type == "model":
            populate_models(c, conn, api, lower, upper, update)

      # Save (commit) the changes
      conn.commit()
      
      # We can also close the connection if we are done with it.
      # Just be sure any changes have been committed or they will be lost.
      conn.close()

      exec_time = time.time() - start_time
      print("\nExecution time:")
      print("--- %s seconds ---" % (exec_time))
      print("--- %s minutes ---" % (round(exec_time/60,2)))
      print("--- %s hours ---" % (round(exec_time/3600,2)))

if __name__ == "__main__":
    main(sys.argv[1:])