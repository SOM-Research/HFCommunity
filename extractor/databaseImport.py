#!/usr/bin/env python
""" Script to retrieve HFH and Git data, and to create and to populate a MariaDB database """
import getopt
import subprocess
import sys
import os
import shutil
import time
from pydriller import Repository
from git.exc import GitCommandError
import mysql.connector
import json
from huggingface_hub import HfApi
import itertools
from cleantext import clean
from datetime import datetime
from dateutil.parser import parse
import dateutil.relativedelta
import pytz


ACCESS_TOKEN = ''
SKIPPED_REPOS = 0

# AUXILIARY FUNCTIONS
def eprint(*args, **kwargs):
      """
      Auxiliar function to print errors to stderr.
      """
      print(*args, file=sys.stderr, **kwargs)

def onerror(func, path, exc_info):
    """
    Error handler for ``shutil.rmtree``. Intended for Windows usage (e.g., `Acces Denied <https://stackoverflow.com/questions/2656322/shutil-rmtree-fails-on-windows-with-access-is-denied>`_)

    If the error is due to an access error (read only file)
    it attempts to add write permission and then retries.

    If the error is for another reason it re-raises the error.
    
    Usage : ``shutil.rmtree(path, onerror=onerror)``
    """
    import stat
    # Is the error an access error?
    if not os.access(path, os.W_OK):
        os.chmod(path, stat.S_IWUSR)
        func(path)
    else:
        raise


# DB CONFIGURATION
def create_connection_mysql():
      """ 
      Configures a database connection to a MySQL/MariaDB database.
      Database configuration file must be located in the path as a JSON file called `db.config`.

      :return: The MySQL connector to the database specified in the configuration file.
      :rtype: `connection.MySQLConnection`_
      """

      # TODO: ... config file with the following structure:
      # {
      #       "host" : HOST,
      #       "port" : PORT,
      #       "user" : USER,
      #       "pass" : PASS,
      #       "database" : SCHEMANAME
      # }

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
      return conn, config['database']


def create_schema_mysql(cursor):
      """ 
      Creates the database schema, following this `ER diagram <https://som-research.github.io/HFCommunity/download.html#er_diagram>`_.

      :param cursor: The MySQL connection cursor to execute operations such as SQL statements.
      :type cursor: `cursor.MySQLCursor`_
      """

      print("Deleting tables...")
      
      cursor.execute('''DROP TABLE IF EXISTS tag''')
      cursor.execute('''DROP TABLE IF EXISTS author''')
      cursor.execute('''DROP TABLE IF EXISTS file''')
      cursor.execute('''DROP TABLE IF EXISTS files_in_repo''')
      cursor.execute('''DROP TABLE IF EXISTS tags_in_repo''')
      cursor.execute('''DROP TABLE IF EXISTS discussion''')
      cursor.execute('''DROP TABLE IF EXISTS model''')
      cursor.execute('''DROP TABLE IF EXISTS dataset''')
      cursor.execute('''DROP TABLE IF EXISTS repository''')
      cursor.execute('''DROP TABLE IF EXISTS commits''')
      cursor.execute('''DROP TABLE IF EXISTS commit_parents''')
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
            CREATE TABLE IF NOT EXISTS repository
            (id VARCHAR(256) PRIMARY KEY, name TEXT, type VARCHAR(7), author VARCHAR(256), sha TEXT, last_modified TEXT, private INTEGER, card_data LONGTEXT, gated INTEGER, likes INTEGER, FOREIGN KEY (author) REFERENCES author(username))
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS file
            (id INTEGER PRIMARY KEY AUTO_INCREMENT, filename VARCHAR(500), repo_id VARCHAR(256), UNIQUE(filename, repo_id), FOREIGN KEY (repo_id) REFERENCES repository(id))
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS tags_in_repo
            (tag_name VARCHAR(256), repo_id VARCHAR(256), PRIMARY KEY(tag_name, repo_id), FOREIGN KEY (tag_name) REFERENCES tag(name), FOREIGN KEY (repo_id) REFERENCES repository(id))
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS discussion
            (num INTEGER, repo_id VARCHAR(256), author VARCHAR(256), title TEXT, status TEXT, created_at TEXT, is_pull_request INTEGER, PRIMARY KEY(num, repo_id), FOREIGN KEY (repo_id) REFERENCES repository, FOREIGN KEY (author) REFERENCES author(username))
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS model
            (id VARCHAR(256) PRIMARY KEY, name TEXT, model_id TEXT, author VARCHAR(256), sha TEXT, last_modified TEXT, private INTEGER, pipeline_tag TEXT, downloads INTEGER, library_name TEXT, likes INTEGER, config LONGTEXT, card_data LONGTEXT, gated INTEGER FOREIGN KEY (repo_id) REFERENCES repository)
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS dataset
            (id VARCHAR(256) PRIMARY KEY, name TEXT, author VARCHAR(256), sha TEXT, last_modified TEXT, private INTEGER, gated INTEGER, citation TEXT, description TEXT, downloads INTEGER, likes INTEGER, card_data LONGTEXT, paperswithcode_id TEXT FOREIGN KEY (repo_id) REFERENCES repository)
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS commits
            (sha VARCHAR(256) PRIMARY KEY, timestamp TEXT, message TEXT, author VARCHAR(256), FOREIGN KEY (author) REFERENCES author(username))
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS commit_parents
            (commit_sha VARCHAR(256), parent_sha VARCHAR(256), PRIMARY KEY(commit_sha, parent_sha), FOREIGN KEY (commit_sha) REFERENCES commits(sha), FOREIGN KEY (parent_sha) REFERENCES commits(sha))
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS files_in_commit
            (sha VARCHAR(256), file_id INTEGER, PRIMARY KEY(sha, file_id), FOREIGN KEY (sha) REFERENCES commits(sha), FOREIGN KEY (file_id) REFERENCES file(id))
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS discussion_event
            (id VARCHAR(256) PRIMARY KEY, repo_id VARCHAR(256), discussion_num INTEGER,  type VARCHAR(64), created_at TEXT, author VARCHAR(256), content TEXT, edited INTEGER, hidden INTEGER, new_status TEXT, summary TEXT, sha TEXT, old_title TEXT, new_title TEXT, full_data LONGTEXT, FOREIGN KEY (sha) REFERENCES commits(sha), FOREIGN KEY (author) REFERENCES author(username), FOREIGN KEY (discussion_num, repo_id) REFERENCES discussion(num, repo_id))
            ''')

      print("Tables created!")


# POPULATE FUNCTIONS
def populate_tags(cursor, conn, tags, repo_name, type):
      """ 
      Importation of tag information. 
      It inserts tag information into the ``tag`` and ``tags_in_repo`` tables.
      
      :param cursor: The MySQL connection cursor to execute operations such as SQL statements.
      :type cursor: `cursor.MySQLCursor`_
      :param conn: The MySQL connector to the database specified in the configuration file. Used to commit changes to fulfill FK restrictions.
      :type conn: `connection.MySQLConnection`_
      :param tags: A list of tag names.
      :type tags: list[str]
      :param repo_name: The name of the repository.
      :type repo_name: str
      :param type: The type of the repository (i.e., model, dataset or space)
      :type type: str
      """
  
      for tag in tags:
            
            cursor.execute('''
            INSERT IGNORE INTO tag (name) VALUES (%s)
            ''', [tag])
            conn.commit() # Commit changes to DB to fulfill FK restriction
            cursor.execute('''
            INSERT IGNORE INTO tags_in_repo (tag_name, repo_id) VALUES (%s, %s)
            ''', (tag, type + '/' + repo_name))


def populate_files(cursor, files, repo_name, type):
      """ 
      Importation of file information.
      It inserts file information into ``file`` table.

      :param cursor: The MySQL connection cursor to execute operations such as SQL statements.
      :type cursor: `cursor.MySQLCursor <https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursor.html>`_
      :param files: A list of filenames.
      :type files: list[str]
      :param repo_name: The name of the repository.
      :type repo_name: str
      :param type: The type of the repository (i.e., model, dataset or space) 
      :type type: str
      """
 
      for file in files:
            if type == "models" or type == "spaces":
                  filename = file.rfilename
            elif type == "datasets":
                  filename = file

            cursor.execute('''
            INSERT IGNORE INTO file (filename, repo_id) VALUES (%s, %s)
            ''', (filename, type + '/' + repo_name))


def populate_dataset_files(cursor, dataset_id, api):
      """ 
      Importation of dataset file information.
      Dataset objects retrieved from ``huggingface_hub`` library (API) do not include the file information of the repository.
      The files can be retrieved using an auxiliar function (``api.list_repo_files``).

      :param cursor: The MySQL connection cursor to execute operations such as SQL statements.
      :type cursor: `cursor.MySQLCursor`_
      :param dataset_id: The name of the dataset repository.
      :type dataset_id: str
      :param api: The huggingface_hub API object pointer.
      :type api: `huggingface_hub.HfApi`_
      """

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
      """ 
      Importation of commit information using PyDriller.
      It inserts commit information into ``commits``, ``author`` and ``files_in_commit`` tables.
      
      :param cursor: The MySQL connection cursor to execute operations such as SQL statements.
      :type cursor: `cursor.MySQLCursor`_
      :param conn: The MySQL connector to the database specified in the configuration file. Used to commit changes to fulfill FK restrictions. Used to commit changes to fulfill FK restrictions.
      :type conn: `connection.MySQLConnection`_
      :param repo_id: The full name (i.e., "owner/repo_name") of the repository.
      :type repo_id: str
      :param type: The type of the repository (i.e., model, dataset or space)
      :type type: str
      """

      # As we have to do some SELECTs to the database, we commit all the INSERTs until now
      conn.commit()

      url_prefix = ""
      if type != "models":
            url_prefix = type + '/'
      

      url = 'https://user:password@huggingface.co/' + url_prefix + repo_id

      # repo_path = "../" + type + "_bare_clone" # WARNING: With this workaround we can just have one process per repo type
      repo_path = type + "_bare_clone"

      try:
            subprocess.check_output(["git",  "clone",  "--bare",  url, repo_path])
      except subprocess.CalledProcessError:
            eprint("HF Repository clone error (repo: " + repo_id + "): authentication error.\n")
            return
      

      # path = os.path.abspath(os.path.join(os.getcwd(), os.pardir)) + '/' + type + "_bare_clone"
      path = os.path.abspath(os.path.join(os.getcwd(), repo_path))

      repo = Repository(path)
      
      num_commits = int(subprocess.check_output(["git",  "rev-list",  "--count", "HEAD"], cwd=path))
      

      # We will skip repos with more than 2k commits or 30k files
      if type == 'datasets' or type == 'model':
            cursor.execute("SELECT COUNT(filename) FROM file WHERE repo_id=%s", [type + '/' + repo_id])
            num_files = cursor.fetchone()
            if num_commits>2000 or num_files[0] > 30000:
                  print("Repo: ", repo_id, " skipped with num_commits: ", num_commits, " and num_files: ", num_files[0])
                  shutil.rmtree(path, ignore_errors=False, onerror=onerror)
                  # os.system("rm -rf " + repo_path)
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
                  
            # os.system("rm -rf " + repo_path)
            shutil.rmtree(path, ignore_errors=False, onerror=onerror)
     
      except GitCommandError:
            eprint("\n")
            eprint("PyDriller Repository clone error (repo: " + repo_id + "): \n", sys.exc_info())
            eprint("\n")
            return


def populate_discussions(cursor, conn, api, repo_name, type):
      """ 
      Importation of discussions information.
      It inserts discussion information into ``discussion``, ``author`` and ``discussion_event`` tables.
      
      :param cursor: The MySQL connection cursor to execute operations such as SQL statements.
      :type cursor: `cursor.MySQLCursor`_
      :param conn: A MySQL connector to the database specified in the configuration file. Used to commit changes to fulfill FK restrictions.
      :type  conn: `connection.MySQLConnection`_
      :param api: The huggingface_hub API object pointer.
      :type api: `huggingface_hub.HfApi`_
      :param repo_name: The name of the repository.
      :type repo_name: str
      :param type: The type of the repository (i.e., model, dataset or space) 
      :type type: str
      """

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
      """ 
      Importation of the information of models.
      It retrieves the whole set of models from HFH, and optionally, it slices the set using the lower and upper params. 
      It inserts model information into ``repository``, ``model`` and ``author`` tables, and calls the populate methods to fill the remaining tables (not including ``populate_datasets`` and ``populate_spaces``).
      
      :param cursor: The MySQL connection cursor to execute operations such as SQL statements.
      :type cursor: `cursor.MySQLCursor`_
      :param conn: A MySQL connector to the database specified in the configuration file. Used to commit changes to fulfill FK restrictions.
      :type conn: `connection.MySQLConnection <https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlconnection.html>`_
      :param api: The huggingface_hub API object pointer.
      :type api: `huggingface_hub.HfApi <https://huggingface.co/docs/huggingface_hub/v0.16.3/en/package_reference/hf_api#huggingface_hub.HfApi>`_
      :param lower: Lower bound of the slicing of the set of models
      :type lower: int
      :param upper: Upper bound of the slicing of the set of models
      :type upper: int
      :param limit_date: Date from which it starts to update the database (e.g., update the database values of the models that have been modified in the last month).
      :type limit_date: datetime
      """

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
      """ 
      Importation of the information of datasets.
      It retrieves the whole set of datasets from HFH, and optionally, it slices the set using the lower and upper params. 
      It inserts dataset information into ``repository``, ``dataset`` and ``author`` tables, and calls the populate methods to fill the remaining tables (not including ``populate_models`` and ``populate_spaces``)..
      
      :param cursor: The MySQL connection cursor to execute operations such as SQL statements.
      :type cursor: `cursor.MySQLCursor`_
      :param conn: A MySQL connector to the database specified in the configuration file. Used to commit changes to fulfill FK restrictions.
      :type conn: `connection.MySQLConnection`_
      :param api: The huggingface_hub API object pointer.
      :type api: `huggingface_hub.HfApi`_
      :param lower: Lower bound of the slicing of the set of datasets
      :type lower: int
      :param upper: Upper bound of the slicing of the set of datasets
      :type upper: int
      :param limit_date: Date from which it starts to update the database (e.g., update the database values of the datasets that have been modified in the last month). 
      :type limit_date: datetime
      """

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
      """ 
      Importation of the information of spaces.
      It retrieves the whole set of spaces from HFH, and optionally, it slices the set using the lower and upper params. 
      It inserts model information into ``repository`` and ``author`` tables, and calls the populate methods to fill the remaining tables (not including ``populate_models`` and ``populate_datasets``).
      
      :param cursor: The MySQL connection cursor to execute operations such as SQL statements.
      :type cursor: `cursor.MySQLCursor`_
      :param conn: A MySQL connector to the database specified in the configuration file. Used to commit changes to fulfill FK restrictions.
      :type conn: `connection.MySQLConnection`_
      :param api: The huggingface_hub API object pointer.
      :type api: `huggingface_hub.HfApi`_
      :param lower: Lower bound of the slicing of the set of spaces
      :type lower: int
      :param upper: Upper bound of the slicing of the set of spaces
      :type upper: int
      :param limit_date: Date from which it starts to update the database (e.g., update the database values of the spaces that have been modified in the last month).
      :type limit_date: datetime
      """

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
      """ Main method. Called when invoking the script. """

      # The execution time is measured and dumped to stdout
      start_time = time.time()

      if len(argv) == 0:
        sys.exit(1)

      try:
            opts, args = getopt.getopt(argv, "t:l:u:c", [])
      except getopt.GetoptError:
            eprint("Wrong usage. USAGE: python databaseImport.py [-c] [-t type] [-l lower_index] [-u upper_index]")
            sys.exit(1)
      
      # Out file
      # out=open('path')
      # print(",",file=out)
      
      token_file = open("read_token", "r")      
      ACCESS_TOKEN = token_file.readline()
      token_file.close()

      # Monthly recovery, we just need the updates of the last month; Can be configured to N months
      limit_date = pytz.UTC.localize(datetime.now() - dateutil.relativedelta.relativedelta(months=1))


      lower = None
      upper = None
      type = ""

      conn, database = create_connection_mysql()

      c = conn.cursor()
      print("connection done")
      
      for opt, arg in opts:
            if opt in ("-t"):
                  type = arg
            if opt in ("-l"):
                  lower = int(arg)
            if opt in ("-u"):
                  upper = int(arg)
            if opt in ("-c"):
                  create_schema_mysql(c)
                  print("Database schema created!")


      # Settings to avoid formatting error when inserting text
      c.execute('SET NAMES utf8mb4')
      c.execute("SET CHARACTER SET utf8mb4")
      c.execute("SET character_set_connection=utf8mb4")

      def check_database_schema(cursor, database):
            """
            Auxiliar function to check if all the tables required are in the database.
            
            :param cursor: The MySQL connection cursor to execute operations such as SQL statements.
            :type cursor: `cursor.MySQLCursor`_
            :param database: MariaDB database name.
            :type database: str
            """
            cursor.execute("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = %s", [database])
            db_tables = list(map(lambda x: x[0], cursor.fetchall()))
            
            hf_tables = ["author", "commit_parents", "commits", "dataset", "discussion", "discussion_event", "file", "files_in_commit", "model", "repository", "tag", "tags_in_repo"]
            
            return set(hf_tables).issubset(db_tables)
      
      tables_exist = check_database_schema(c, database)

      if not tables_exist:
            eprint("There are some (or all) tables required missing in the target database.\nPlease, if the tables are not created following the HFCommunity schema use the -c flag.\nUSAGE: python databaseImport.py -c [-t type]")
            sys.exit(1)
      
      exit(1)


      api = HfApi()

      # Import by type. 
      if type == "dataset":
            populate_datasets(c, conn, api, lower, upper, limit_date)
      elif type == "space":
            populate_spaces(c, conn, api, lower, upper, limit_date)
      elif type == "model":
            populate_models(c, conn, api, lower, upper, limit_date)
      else:
            populate_datasets(c, conn, api, lower, upper, limit_date)
            populate_spaces(c, conn, api, lower, upper, limit_date)
            populate_models(c, conn, api, lower, upper, limit_date)

      # Save (commit) the changes
      conn.commit()
      
      # We can also close the connection if we are done with it.
      # Just be sure any changes have been committed or they will be lost.
      conn.close()

      # TODO: Workaround for skipped repos
      if type == "dataset":
            print("SKIPPED REPOS: ", SKIPPED_REPOS)

      # Printing execution time...
      exec_time = time.time() - start_time
      print("\nExecution time:")
      print("--- %s seconds ---" % (exec_time))
      print("--- %s minutes ---" % (round(exec_time/60,2)))
      print("--- %s hours ---" % (round(exec_time/3600,2)))

if __name__ == "__main__":
    main(sys.argv[1:])