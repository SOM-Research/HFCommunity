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
from huggingface_hub import HfApi
import itertools
from cleantext import clean
from datetime import datetime
from dateutil.parser import parse
import dateutil.relativedelta
import pytz


ACCESS_TOKEN = ''
SKIPPED_REPOS = 0

# ERROR PRINTING
def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


# DB CONFIGURATION
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


# POPULATE FUNCTIONS
def populate_tags(cursor, conn, tags, repo_name, type):
      """ importation of tag information """
  
      for tag in tags:
            
            cursor.execute('''
            INSERT IGNORE INTO tag (name) VALUES (%s)
            ''', [tag])
            conn.commit() # Commit changes to DB to fulfill FK restriction
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
      """ Importation of dataset file information """

      # Sometimes list_repo_files does not retreive info at first. We try 5 times.
      n = 0
      while n < 5:
            try:
                  populate_files(cursor, api.list_repo_files(repo_id=dataset_id, repo_type="dataset", use_auth_token=ACCESS_TOKEN), dataset_id, "datasets")
                  n = 5
            except:
                  eprint("---------DATASET FILES ERROR---------")
                  eprint("ERROR: Unknown (Repo: (dataset) " + dataset_id + "). Printing exception...")
                  n = 5


def populate_commits(cursor, conn, repo_id, type):
      """ importation of commit information using PyDriller """

      # As we have to do some SELECTs to the database, we commit all the INSERTs until now
      conn.commit()

      url_prefix = ""
      if type != "models":
            url_prefix = type + '/'
      

      url = 'https://user:password@huggingface.co/' + url_prefix + repo_id

      repo_path = "../" + type + "_bare_clone" # WARNING: With this workaround we can just have one process per repo type

      try:
            subprocess.check_output(["git",  "clone",  "--bare",  url, repo_path])
      except subprocess.CalledProcessError:
            eprint("HF Repository clone error (repo: " + repo_id + "): authentication error.\n")
            return

      # Some repos are like 'author/reponame' and others just 'reponame'
      if '/' in repo_id:
            # We get just 'reponame'
            repo_folder = repo_id.split('/')[1] + '.git'
      else:
            repo_folder = repo_id + '.git'

      path = os.path.abspath(os.path.join(os.getcwd(), os.pardir)) + '/' + type + "_bare_clone"

      repo = Repository(path)
      
      num_commits = int(subprocess.check_output(["git",  "rev-list",  "--count", "HEAD"], cwd=path))
      

      # We will skip repos with more than 2k commits or 30k files
      if type == 'datasets' or type == 'model':
            cursor.execute("SELECT COUNT(filename) FROM file WHERE repo_id=%s", [type + '/' + repo_id])
            num_files = cursor.fetchone()
            if num_commits>2000 or num_files[0] > 30000:
                  print("Repo: ", repo_id, " skipped with num_commits: ", num_commits, " and num_files: ", num_files[0])
                  os.system("rm -rf " + repo_path)
                  global SKIPPED_REPOS
                  SKIPPED_REPOS = SKIPPED_REPOS + 1
                  return

      # Start with the commit importation
      try:
            cursor.execute("SELECT filename, id FROM file WHERE repo_id=%s", [type + '/' + repo_id])
            repo_files = dict(cursor.fetchall())

            for commit in repo.traverse_commits():
                  
                  cursor.execute('''
                  INSERT IGNORE INTO author (username, source) VALUES (%s, %s)
                  ''', (commit.author.name, "commit"))
                  # Commit author
                  conn.commit()
                  cursor.execute('''
                  INSERT IGNORE INTO commits (sha, timestamp, message, author) VALUES (%s, %s, %s, %s)
                  ''', (commit.hash, commit.author_date, commit.msg, commit.author.name))
                  
                  # Commit the commit
                  conn.commit()

                  try:
                        # Check whether the files in the commit exist in the repo 
                        # (the file could be deleted, or renamed, so we think we should only track current files)
                        commit_files      = [file.new_path for file in commit.modified_files]
                        commit_files_dict = {key:repo_files[key] for key in commit_files if key in repo_files.keys()}

                        for key, value in commit_files_dict.items():
                              cursor.execute('''
                                    INSERT IGNORE INTO files_in_commit (sha, file_id) VALUES (%s, %s)
                                    ''', (commit.hash, value))
                        
                        n = len(commit_files) - len(commit_files_dict)

                        if n > 0:
                              eprint(str(n) + " file(s) of repo (type: " + type + ", sha: ", commit.hash, ") " + repo_id + " not found.")

                  except AttributeError as error:
                        eprint("File searching error. Printing Exception:")
                        eprint(error)
                  
            os.system("rm -rf " + repo_path)
     
      except GitCommandError:
            eprint("\n")
            eprint("PyDriller Repository clone error (repo: " + repo_id + "): \n", sys.exc_info())
            eprint("\n")
            return


def populate_discussions(cursor, conn, api, repo_name, type):
      """ Retrieve discussion from a repo and populate discussion table """

      # If discussions are disabled, it is thrown an HTTPError Exception
      try:
            for discussion in api.get_repo_discussions(repo_id=repo_name, repo_type=type, token=ACCESS_TOKEN):
                  
                  details = api.get_discussion_details(repo_id=repo_name, discussion_num=discussion.num, repo_type=type, token=ACCESS_TOKEN)

                  author_name = details.__dict__.get("author")

                  cursor.execute('''
                  INSERT IGNORE INTO author (username, source) VALUES (%s, %s)
                  ''', (author_name, "hf"))

                  # commit the discussion author
                  conn.commit()

                  cursor.execute('''
                        INSERT INTO discussion (num, repo_id, author, title, status, created_at, is_pull_request) VALUES (%s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE status = values(status)
                        ''', (details.__dict__.get("num"), type + 's/' + repo_name, author_name, details.__dict__.get("title"), details.__dict__.get("status"), details.__dict__.get("created_at"), details.__dict__.get("is_pull_request")))
                  
                  # commit the discussion
                  conn.commit()

                  for event in details.events:
                        author_name = event.__dict__.get("author")

                        author = event.__dict__.get('_event').get('author')

                        # Insertion of event author
                        if author is not None:
                              cursor.execute('''
                                    INSERT INTO author (username, avatar_url, is_pro, fullname, type, source) VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE avatar_url = values(avatar_url), is_pro = values(is_pro), fullname = values(fullname), type = values(type), source = values(source)
                                    ''', (author.get("name"), author.get("avatarUrl"), author.get("isPro"), author.get("fullname"), author.get("type"), "hf"))
                              # commit author
                              conn.commit()

                        # Insertion of event
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
      except Exception as e:
            eprint("---------DISCUSSIONS ERROR----------")
            eprint("Printing exception:")
            eprint(e)
            eprint("Repo: ", repo_name)


def populate_models(cursor, conn, api, lower, upper, limit_date):
      """ importation of the full information of models """

      print("Retrieving full information of models...")
      # list(iter()) is a workaround until pagination is released in v0.14
      # Apply sort to get the most recent repos (in descending order; direction = -1); Sort just returns 10k repos, use sorted method instead
      hub_models = list(iter(api.list_models(full=True, cardData=True, fetch_config=True, use_auth_token=ACCESS_TOKEN)))
      models = sorted(hub_models, key=lambda d: d.__dict__.get("lastModified"), reverse=True)
      print("Info retrieved! Gathered", len(hub_models), "models")
      
      print("Starting population into database...")

      i = 0
      full_update = True

      for model in itertools.islice(models, lower, upper):
            
            model_id = "models/" + model.id

            # Gather just those with modifications within the limit_date (e.g., last month)
            if not full_update or parse(model.__dict__.get("lastModified")) < limit_date:
                  full_update = False
                  cursor.execute('''
                  UPDATE repository 
                  SET likes=%s
                  WHERE id=%s
                  ''', (model.__dict__.get("likes"), model_id))
                  cursor.execute('''
                  UPDATE model 
                  SET downloads=%s
                  WHERE model_id=%s
                  ''', (model.__dict__.get("downloads"), model_id))
                  continue
            
            i += 1

            # TODO: Do workaround
            if model.__dict__.get("id") in ["AmirHussein/icefall-asr-mgb2-conformer_ctc-2022-27-06", "datablations/lm1-misc","tktkdrrrrrrrrrrr/CivitAI_model_info"]:
                  continue

            if (model.__dict__.get("author") != None):
                  cursor.execute('''
                  INSERT IGNORE INTO author (username, source) VALUES (%s, %s)
                  ''', (model.author, "hf_owner"))
                  conn.commit()

            # Cleaning of 'config' (strange characters, emojis, etc.)
            config = str(model.__dict__.get("config"))
            config = clean(config, no_emoji=True)

            cursor.execute('''
            INSERT INTO repository (id, name, type, author, sha, last_modified, private, card_data, gated, likes) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE name = values(name), type = values(type), author = values(author), sha = values(sha), last_modified= values(last_modified), private = values(private), card_data = values(card_data), gated = values(gated), likes = values(likes)
            ''', (model_id, model.__dict__.get("id"), "model", model.__dict__.get("author"), model.__dict__.get("sha"), model.__dict__.get("lastModified"), model.__dict__.get("private"), str(model.__dict__.get("cardData")), model.__dict__.get("gated"), model.__dict__.get("likes")))

            # Commit to the DB because of the FK constraint
            conn.commit()

            cursor.execute('''
            INSERT INTO model (model_id, pipeline_tag, downloads, library_name, config) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE pipeline_tag = values(pipeline_tag), downloads = values(downloads), library_name = values(library_name), config = values(config)
            ''', (model_id, model.__dict__.get("pipeline_tag"), model.__dict__.get("downloads"), model.__dict__.get("library_name"), config))
            
            
            populate_tags(cursor, conn, model.tags, model.modelId, "models")
            populate_files(cursor, model.siblings, model.modelId, "models")
            conn.commit() # Commit of files
            populate_commits(cursor, conn, model.modelId, "models")
            populate_discussions(cursor, conn, api, model.modelId, "model")


      print(i, "models updated!")
      print("Models population finished!")


def populate_datasets(cursor, conn, api, lower, upper, limit_date):
      """ importation of dataset data """

      print("Retrieving information of datasets...")

      # cardData deprecated in v0.14, use just full
      hub_datasets = list(iter(api.list_datasets(full=True, use_auth_token=ACCESS_TOKEN)))
      datasets = sorted(hub_datasets, key=lambda d: d.__dict__.get("lastModified"), reverse=True)

      print("Info retrieved! Gathered", len(hub_datasets), "datasets")

      i = 0
      full_update = True

      for dataset in itertools.islice(datasets, lower, upper):

            dataset_id = "datasets/" + dataset.__dict__.get("id")

            if not full_update or parse(dataset.__dict__.get("lastModified")) < limit_date:
                  full_update = False
                  cursor.execute('''
                  UPDATE repository 
                  SET likes=%s
                  WHERE id=%s
                  ''', (dataset.__dict__.get("likes"), dataset_id))
                  cursor.execute('''
                  UPDATE dataset
                  SET downloads=%s
                  WHERE dataset_id=%s
                  ''', (dataset.__dict__.get("downloads"), dataset_id))
                  continue

            i += 1
            
            # This repo is huge, it is needed a workaround when collecting the commits;
            if dataset.__dict__.get("id") in ["ywchoi/mdpi_sept10", "ACL-OCL/acl-anthology-corpus", "uripper/ProductScreenshots", "gozfarb/ShareGPT_Vicuna_unfiltered","deepsynthbody/deepfake_ecg_full_train_validation_test"]:
                  continue

            if (dataset.author != None):
                  cursor.execute('''
                  INSERT IGNORE INTO author (username, source) VALUES (%s, %s)
                  ''', (dataset.author, "hf_owner"))
                  conn.commit()

            # TODO: Change gated to String
            cursor.execute('''
            INSERT INTO repository (id, name, type, author, sha, last_modified, private, card_data, gated, likes) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE name = values(name), type = values(type), author = values(author), sha = values(sha), last_modified= values(last_modified), private = values(private), card_data = values(card_data), gated = values(gated), likes = values(likes)
            ''', (dataset_id, dataset.__dict__.get("id"), "dataset", dataset.__dict__.get("author"), dataset.__dict__.get("sha"), dataset.__dict__.get("lastModified"), dataset.__dict__.get("private"), str(dataset.__dict__.get("cardData")), None, dataset.__dict__.get("likes")))
            conn.commit()

            cursor.execute('''
            INSERT INTO dataset (dataset_id, description, citation, paperswithcode_id, downloads) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE description = values(description), citation = values(citation), paperswithcode_id = values(paperswithcode_id), downloads = values(downloads)
            ''', (dataset_id, dataset.__dict__.get("description"), dataset.__dict__.get("citation"), dataset.__dict__.get("paperswithcode_id"), dataset.__dict__.get("downloads")))         
            

            # dataset.siblings is always None, but we can use another API endpoint
            populate_tags(cursor, conn, dataset.tags, dataset.id, "datasets")
            populate_dataset_files(cursor, dataset.id, api)                 
            conn.commit()
            populate_commits(cursor, conn, dataset.id, "datasets")
            populate_discussions(cursor, conn, api, dataset.id, "dataset")

      
      print(i, "datasets updated!")
      print("Dataset information retrieved!")


def populate_spaces(cursor, conn, api, lower, upper, limit_date):
      """ importation of space data """

      print("Retrieving information of spaces...")
      hub_spaces = list(iter(api.list_spaces(full=True, use_auth_token=ACCESS_TOKEN)))
      spaces = sorted(hub_spaces, key=lambda d: d.__dict__.get("lastModified"), reverse=True)

      print("Info retrieved! Gathered", len(hub_spaces), "spaces")

      i = 0
      full_update = True

      for space in itertools.islice(spaces, lower, upper):
            
            space_id = "spaces/" + space.__dict__.get("id")

            if not full_update or parse(space.__dict__.get("lastModified")) < limit_date:
                  full_update = False
                  cursor.execute('''
                  UPDATE repository 
                  SET likes=%s
                  WHERE id=%s
                  ''', (space.__dict__.get("likes"), space_id))
                  continue
            
            i += 1

            # This repo always give error with PyDriller, we'll have to take a look -> in web ERROR in deploying
            if space.__dict__.get("id") in ["mfrashad/ClothingGAN", "mfrashad/CharacterGAN", "fdfdd12345628/Tainan", "patent/demo1"]:
                  continue
            
            if (space.__dict__.get("author") != None):
                  cursor.execute('''
                  INSERT IGNORE INTO author (username, source) VALUES (%s, %s)
                  ''', (space.author, "hf_owner"))
                  conn.commit()

            cursor.execute('''
            INSERT INTO repository (id, name, type, author, sha, last_modified, private, card_data, gated, likes) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE name = values(name), type = values(type), author = values(author), sha = values(sha), last_modified= values(last_modified), private = values(private), card_data = values(card_data), gated = values(gated), likes = values(likes)
            ''', (space_id, space.__dict__.get("id"), "space", space.__dict__.get("author"), space.__dict__.get("sha"), space.__dict__.get("lastModified"), space.__dict__.get("private"), str(space.__dict__.get("cardData")), space.__dict__.get("gated"), space.__dict__.get("likes")))
            
            populate_tags(cursor, conn, space.tags, space.id, "spaces")
            populate_files(cursor, space.siblings, space.id, "spaces")
            conn.commit()
            populate_commits(cursor, conn, space.id, "spaces")
            populate_discussions(cursor, conn, api, space.id, "space")

      print(i, "spaces updated!")
      print("Spaces information retrieved!")



def main(argv):

      start_time = time.time()

      if len(argv) == 0:
        sys.exit(1)

      try:
            opts, args = getopt.getopt(argv, "t:l:u:", [])
      except getopt.GetoptError:
            eprint("Wrong usage. USAGE: python databaseImport.py -t type -l lower_index -u upper_index")
            sys.exit(1)
      
      token_file = open("read_token", "r")      
      ACCESS_TOKEN = token_file.readline()
      token_file.close()

      # Monthly recovery, we just need the updates of the last month
      limit_date = pytz.UTC.localize(datetime.now() - dateutil.relativedelta.relativedelta(months=2)) # TODO: Change to 1


      lower = None
      upper = None
      type = ""
      
      for opt, arg in opts:
            if opt in ("-t"):
                  type = arg
            if opt in ("-l"):
                  lower = int(arg)
            if opt in ("-u"):
                  upper = int(arg)

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
            populate_datasets(c, conn, api, lower, upper, limit_date)
      elif type == "space":
            populate_spaces(c, conn, api, lower, upper, limit_date)
      elif type == "model":
            populate_models(c, conn, api, lower, upper, limit_date)
      elif type == "all":
            populate_datasets(c, conn, api, lower, upper, limit_date)
            populate_spaces(c, conn, api, lower, upper, limit_date)
            populate_models(c, conn, api, lower, upper, limit_date)

      # Save (commit) the changes
      conn.commit()
      
      # We can also close the connection if we are done with it.
      # Just be sure any changes have been committed or they will be lost.
      conn.close()

      if type == "dataset":
            print("SKIPPED REPOS: ", SKIPPED_REPOS)

      exec_time = time.time() - start_time
      print("\nExecution time:")
      print("--- %s seconds ---" % (exec_time))
      print("--- %s minutes ---" % (round(exec_time/60,2)))
      print("--- %s hours ---" % (round(exec_time/3600,2)))

if __name__ == "__main__":
    main(sys.argv[1:])