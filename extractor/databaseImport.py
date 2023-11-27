#!/usr/bin/env python
""" Script to retrieve HFH and Git data, and to create and to populate a MariaDB database. """
import getopt
import subprocess
import sys
import os
import shutil
import time
from pydriller import Repository
from git.exc import GitCommandError
import mysql.connector
from mysql.connector import errorcode
import json
from huggingface_hub import HfApi
from huggingface_hub.utils import (
    HfHubHTTPError,
    GatedRepoError,
    RepositoryNotFoundError
    )
from cleantext import clean
from datetime import datetime
import dateutil.relativedelta
import pytz
import requests
import hashlib


ACCESS_TOKEN = ''
SKIPPED_REPOS = 0
NUM_COMMITS = -1
NUM_FILES = -1

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
      
      hf_tables = ["author", "commit_parents", "commits", "dataset", "discussion", "conflicting_files_discussion", "discussion_event", "repo_file", "modified_file", "files_in_commit", "model", "repository", "tag", "tags_in_repo", "space", "datasets_in_space", "models_in_space"]
      
      return set(hf_tables).issubset(db_tables)


def read_config():
      """
      Function that retrieves configuration JSON from hfc.config.
      
      :returns config: JSON object containing all configuration parameters.
      :rtype: JSON
      """
      if os.path.exists('hfc.config'):
            with open('hfc.config') as hfc:
                  config = json.load(hfc)
      else:
            eprint('hfc.config file not found!')
            sys.exit(1)
      return config


def validate_token(token):
            """
            Auxiliar function to validate the token placed in hfc.config.

            :param str token: Hugging Face Hub `API token <https://huggingface.co/docs/hub/security-tokens>`_
            """
            url = "https://huggingface.co/api/whoami-v2"
            bearer_token = "Bearer " + token
            headers = {"Authorization": bearer_token}
            response = requests.get(url, headers=headers)
            status_code = response.status_code

            if status_code != 200:
                  if status_code == 401:
                        eprint("HFH token is incorrect, please place a valid HFH token in the 'hfh_token' parameter of the hfc.config file.")
                        sys.exit(1)
                  else:
                        eprint("Verification of HFH token. Return HTTP status code for whoami endpoint:" + str(status_code))
                        eprint("Please place a valid HFH token in the 'hfh_token' parameter of the hfc.config file.")
                        sys.exit(1)
            

# DB CONFIGURATION
def create_connection_mysql():
      """ 
      Configures a database connection to a MySQL/MariaDB database.
      Database configuration file must be located in the path as a JSON file called `hfc.config`.

      :return: Tuple containing the MySQL connector to the database specified in the configuration file and the database name.
      :rtype: (`connection.MySQLConnection`_, str)
      """

      conn = None
      config = read_config()
      
      try :
            conn = mysql.connector.connect(user=config['user'], password=config['pass'], host=config['host'], port=config['port'], database=config['database'])
      except KeyError as ke:
            eprint("Missing some mandatory parameters in hfc.config. Don't forget to specify 'user', 'pass', 'host', 'port' and 'database'.")
            sys.exit(1)
      except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                  eprint("Error in connection to the MariaDB. Something is wrong with your user name or password.")
                  sys.exit(1)
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                  print("Database specified in hfc.config does not exist. Creating such database...")
                  conn = mysql.connector.connect(user=config['user'], password=config['pass'], host=config['host'], port=config['port'])
                  conn.cursor().execute(f"CREATE DATABASE IF NOT EXISTS {config['database']};")
                  print("Database created!")
                  conn.cursor().execute(f"USE {config['database']};")
            else:
                  eprint(err)
                  sys.exit(1)
      return conn, config['database']


def create_schema_mysql(cursor):
      """ 
      Creates the database schema, following this `ER diagram <https://som-research.github.io/HFCommunity/download.html#er_diagram>`_.

      :param cursor: The MySQL connection cursor to execute operations such as SQL statements.
      :type cursor: `cursor.MySQLCursor`_
      """

      print("Deleting tables...")
      
      # We have to delete the tables in order, to not break foreign key restrictions
      cursor.execute('''DROP TABLE IF EXISTS tags_in_repo''')
      cursor.execute('''DROP TABLE IF EXISTS tag''')
      cursor.execute('''DROP TABLE IF EXISTS files_in_commit''')
      cursor.execute('''DROP TABLE IF EXISTS commit_parents''')
      cursor.execute('''DROP TABLE IF EXISTS conflicting_files_discussion''')
      cursor.execute('''DROP TABLE IF EXISTS modified_file''')
      cursor.execute('''DROP TABLE IF EXISTS repo_file''')
      cursor.execute('''DROP TABLE IF EXISTS models_in_space''') 
      cursor.execute('''DROP TABLE IF EXISTS datasets_in_space''')
      cursor.execute('''DROP TABLE IF EXISTS model''')
      cursor.execute('''DROP TABLE IF EXISTS dataset''')
      cursor.execute('''DROP TABLE IF EXISTS space''') 
      cursor.execute('''DROP TABLE IF EXISTS discussion_event''')
      cursor.execute('''DROP TABLE IF EXISTS discussion''')
      cursor.execute('''DROP TABLE IF EXISTS commits''')
      cursor.execute('''DROP TABLE IF EXISTS repository''')
      cursor.execute('''DROP TABLE IF EXISTS author''')
      
      
      

      print("Creating tables...")

      cursor.execute('''
            CREATE TABLE IF NOT EXISTS tag
            (name VARCHAR(256) PRIMARY KEY)
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS author
            (username VARCHAR(256) PRIMARY KEY, avatar_url TEXT, is_pro INTEGER, fullname TEXT, type VARCHAR(64), source VARCHAR(256))
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS repository
            (id VARCHAR(256) PRIMARY KEY, name TEXT, type VARCHAR(7), author VARCHAR(256), sha TEXT, last_modified DATETIME, private INTEGER, card_data LONGTEXT, gated INTEGER, likes INTEGER, disabled INTEGER, FOREIGN KEY (author) REFERENCES author(username))
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS repo_file
            (id CHAR(64) PRIMARY KEY, filename TEXT, repo_id VARCHAR(256), size BIGINT, blob_id VARCHAR(256), lfs_size BIGINT, lfs_sha VARCHAR(256), lfs_pointer_size BIGINT, FOREIGN KEY (repo_id) REFERENCES repository(id))
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS tags_in_repo
            (tag_name VARCHAR(256), repo_id VARCHAR(256), PRIMARY KEY(tag_name, repo_id), FOREIGN KEY (tag_name) REFERENCES tag(name), FOREIGN KEY (repo_id) REFERENCES repository(id))
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS model
            (model_id VARCHAR(256) PRIMARY KEY, pipeline_tag TEXT, downloads INTEGER, library_name TEXT, likes INTEGER, config LONGTEXT, FOREIGN KEY (model_id) REFERENCES repository(id))
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS dataset
            (dataset_id VARCHAR(256) PRIMARY KEY, description LONGTEXT, citation TEXT, paperswithcode_id TEXT, downloads INTEGER, FOREIGN KEY (dataset_id) REFERENCES repository(id))
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS space
            (space_id VARCHAR(256) PRIMARY KEY, sdk VARCHAR(128), stage VARCHAR(32), hardware VARCHAR(32), requested_hw VARCHAR(32), sleep_time INTEGER, storage VARCHAR(32), runtime_raw TEXT, FOREIGN KEY (space_id) REFERENCES repository(id))
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS models_in_space
            (model_id VARCHAR(256), space_id VARCHAR(256), PRIMARY KEY(model_id, space_id), FOREIGN KEY (model_id) REFERENCES model(model_id), FOREIGN KEY (space_id) REFERENCES space(space_id))
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS datasets_in_space
            (dataset_id VARCHAR(256), space_id VARCHAR(256), PRIMARY KEY(dataset_id, space_id), FOREIGN KEY (dataset_id) REFERENCES dataset(dataset_id), FOREIGN KEY (space_id) REFERENCES space(space_id))
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS commits
            (sha VARCHAR(256) PRIMARY KEY, repo_id VARCHAR(256), message LONGTEXT, author_date DATETIME, author_tz INTEGER, committer_date DATETIME, committer_tz INTEGER, in_main_branch INTEGER, insertions INTEGER, deletions INTEGER, author_name VARCHAR(256), committer_name VARCHAR(256), source VARCHAR(32), FOREIGN KEY (author_name) REFERENCES author(username), FOREIGN KEY (repo_id) REFERENCES repository(id), FOREIGN KEY (committer_name) REFERENCES author(username))
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS discussion
            (num INTEGER, repo_id VARCHAR(256), author VARCHAR(256), title TEXT, status TEXT, created_at DATETIME, is_pull_request INTEGER, target_branch VARCHAR(64), merge_commit_oid VARCHAR(256), diff LONGTEXT, git_reference TEXT, PRIMARY KEY(num, repo_id), FOREIGN KEY (repo_id) REFERENCES repository(id), FOREIGN KEY (author) REFERENCES author(username), FOREIGN KEY (merge_commit_oid) REFERENCES commits(sha))
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS conflicting_files_discussion
            (id INTEGER PRIMARY KEY AUTO_INCREMENT, num INTEGER, repo_id VARCHAR(256), filename TEXT, repo_file_id CHAR(64), FOREIGN KEY (repo_id) REFERENCES repository(id), FOREIGN KEY (num, repo_id) REFERENCES discussion(num, repo_id), FOREIGN KEY (repo_file_id) REFERENCES repo_file(id))
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS commit_parents
            (commit_sha VARCHAR(256), parent_sha VARCHAR(256), PRIMARY KEY(commit_sha, parent_sha), FOREIGN KEY (commit_sha) REFERENCES commits(sha), FOREIGN KEY (parent_sha) REFERENCES commits(sha))
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS modified_file
            (modified_file_id CHAR(64) PRIMARY KEY, repo_file_id CHAR(64), rfilename TEXT, old_path VARCHAR(256), new_path VARCHAR(256), change_type VARCHAR(32), diff LONGTEXT, added_lines INTEGER, deleted_lines INTEGER, nloc INTEGER, FOREIGN KEY (repo_file_id) REFERENCES repo_file(id))
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS files_in_commit
            (sha VARCHAR(256), modified_file_id CHAR(64), PRIMARY KEY(sha, modified_file_id), FOREIGN KEY (sha) REFERENCES commits(sha), FOREIGN KEY (modified_file_id) REFERENCES modified_file(modified_file_id))
            ''')
      cursor.execute('''
            CREATE TABLE IF NOT EXISTS discussion_event
            (id VARCHAR(256) PRIMARY KEY, repo_id VARCHAR(256), discussion_num INTEGER,  event_type VARCHAR(64), created_at DATETIME, author VARCHAR(256), content TEXT, edited INTEGER, hidden INTEGER, new_status TEXT, summary TEXT, sha VARCHAR(256), old_title TEXT, new_title TEXT, full_data LONGTEXT, FOREIGN KEY (sha) REFERENCES commits(sha), FOREIGN KEY (author) REFERENCES author(username), FOREIGN KEY (discussion_num, repo_id) REFERENCES discussion(num, repo_id))
            ''')

      print("Tables created!")


# POPULATE FUNCTIONS
def populate_tags(cursor, conn, tags, repo_name, repo_type):
      """ 
      Importation of tag information. 
      It inserts tag information into the ``tag`` and ``tags_in_repo`` tables.
      
      :param cursor: The MySQL connection cursor to execute operations such as SQL statements.
      :type cursor: `cursor.MySQLCursor`_
      :param conn: The MySQL connector to the database specified in the configuration file. Used to commit changes to fulfill FK restrictions.
      :type conn: `connection.MySQLConnection`_
      :param tags: A list of tag names.
      :type tags: list[str]
      :param repo_name: The full name (i.e., "owner/repo_name") of the repository.
      :type repo_name: str
      :param repo_type: The type of the repository (i.e., model, dataset or space)
      :type repo_type: str
      """
  
      for tag in tags:
            
            cursor.execute('''
            INSERT IGNORE INTO tag (name) VALUES (%s)
            ''', [tag])
            conn.commit() # Commit changes to DB to fulfill FK restriction
            cursor.execute('''
            INSERT IGNORE INTO tags_in_repo (tag_name, repo_id) VALUES (%s, %s)
            ''', (tag, repo_type + '/' + repo_name))


def populate_files(cursor, api, repo_name, repo_type):
      """ 
      Importation of file information.
      It inserts file information into ``repo_file`` table.

      :param cursor: The MySQL connection cursor to execute operations such as SQL statements.
      :type cursor: `cursor.MySQLCursor <https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursor.html>`_
      :param repo_name: The full name (i.e., "owner/repo_name") of the repository.
      :type repo_name: str
      :param repo_type: The type of the repository (i.e., model, dataset or space) 
      :type repo_type: str
      """

      try:
            files = api.repo_info(repo_name, repo_type=repo_type, files_metadata=True, token=ACCESS_TOKEN).siblings
      except (GatedRepoError, RepositoryNotFoundError, HfHubHTTPError) as err:
            eprint("------HUB FILE REPO_INFO ERROR------")
            eprint("Error:", repr(err), "\n")
      else: 
            # A repository can be empty (e.g.,"shredder-31/Question_generation_Fine_tuning_llama_7B")
            if files: 
                  repo_id = repo_type + 's/' + repo_name
                  for file in files:
                        # Calculate hash from filename (not relative: "data/filename") and repo_id (not repo_name)
                        repo_file_id = hashlib.sha256((repo_id + '_' + file.rfilename).encode("utf-8")).hexdigest()
                        if file.lfs is None:
                              cursor.execute('''
                              INSERT INTO repo_file (id, filename, repo_id, size, blob_id) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE size = values(size), blob_id = values(blob_id)
                              ''', (repo_file_id, file.rfilename, repo_id, file.size, file.blob_id))
                        else:
                              cursor.execute('''
                              INSERT INTO repo_file (id, filename, repo_id, size, blob_id, lfs_size, lfs_sha, lfs_pointer_size) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE size = values(size), blob_id = values(blob_id), lfs_size = values(lfs_size), lfs_sha = values(lfs_sha), lfs_pointer_size = values(lfs_pointer_size) 
                              ''', (repo_file_id, file.rfilename, repo_id, file.size, file.blob_id, file.lfs.get("size"), file.lfs.get("sha256"), file.lfs.get("pointer_size")))


def populate_commits(cursor, conn, repo_name, repo_type):
      """ 
      Importation of commit information using PyDriller.
      It inserts commit information into ``commits``, ``author``, ``modified_file`` and ``files_in_commit`` tables.
      
      :param cursor: The MySQL connection cursor to execute operations such as SQL statements.
      :type cursor: `cursor.MySQLCursor`_
      :param conn: The MySQL connector to the database specified in the configuration file. Used to commit changes to fulfill FK restrictions. Used to commit changes to fulfill FK restrictions.
      :type conn: `connection.MySQLConnection`_
      :param repo_name: The full name (i.e., "owner/repo_name") of the repository.
      :type repo_name: str
      :param repo_type: The type of the repository (i.e., model, dataset or space)
      :type repo_type: str
      """

      # As we have to do some SELECTs to the database, we commit all the INSERTs until now
      conn.commit()

      # URLs of repos in HFH have prefix of 'datasets/' or 'spaces/', but not for models
      url_prefix = ""
      if repo_type != "models":
            url_prefix = repo_type + '/'
      
      # Some gated repos require user + psswd info, we collect only public repos, but the url must contain some login info (which will fail) to not generate an exception
      # Now (v1.1, Nov. '23) using ACCESS_TOKEN because of Git psswd deprecation: https://huggingface.co/blog/password-git-deprecation
      url = 'https://user:' + ACCESS_TOKEN + '@huggingface.co/' + url_prefix + repo_name 

      # repo_path = "../" + type + "_bare_clone" # WARNING: With this workaround we can just have one process per repo type
      repo_path = repo_type + "_bare_clone"

      result = subprocess.run(["git",  "clone",  "--bare",  url, repo_path], capture_output=True)
      if result.returncode != 0: # Repo is gated, private or not found
            # result.stderr is a bytes object. Decode converts it as string
            eprint("------------CLONE ERROR-------------")
            eprint("Error (repo: " +  repo_name + "): " + result.stderr.decode("utf-8"))
            return
      

      # path = os.path.abspath(os.path.join(os.getcwd(), os.pardir)) + '/' + type + "_bare_clone"
      path = os.path.abspath(os.path.join(os.getcwd(), repo_path))

      repo = Repository(path)
      
      try:
            num_commits = int(subprocess.check_output(["git",  "rev-list",  "--count", "HEAD"], cwd=path))
      except subprocess.CalledProcessError as Err:
            eprint("-------------GIT ERROR--------------")
            eprint("Error (repo: " +  repo_name + "): " + str(Err))
            shutil.rmtree(path, ignore_errors=False, onerror=onerror)
            return

      

      # We will skip repos specified by number of commits or number of files
      if not (NUM_FILES == -1 and NUM_COMMITS == -1):
            cursor.execute("SELECT COUNT(filename) FROM repo_file WHERE repo_id=%s", [repo_type + '/' + repo_name])
            num_files = cursor.fetchone()
            if num_commits > NUM_COMMITS or num_files[0] > NUM_FILES:
                  print("Repo: ", repo_name, " skipped with num_commits: ", num_commits, " and num_files: ", num_files[0])
                  shutil.rmtree(path, ignore_errors=False, onerror=onerror)
                  global SKIPPED_REPOS
                  SKIPPED_REPOS = SKIPPED_REPOS + 1
                  return

      # Start with the commit importation
      try:
            cursor.execute("SELECT filename, id FROM repo_file WHERE repo_id=%s", [repo_type + '/' + repo_name])
            repo_files = dict(cursor.fetchall())

            for commit in repo.traverse_commits():
                  
                  cursor.execute('''
                  INSERT IGNORE INTO author (username, source) VALUES (%s, %s)
                  ''', (commit.author.name, "commit"))
                  cursor.execute('''
                  INSERT IGNORE INTO author (username, source) VALUES (%s, %s)
                  ''', (commit.committer.name, "commit"))
                  # Commit author & committer
                  conn.commit() 
                  
                  cursor.execute('''
                  INSERT IGNORE INTO commits (sha, repo_id, author_date, author_tz, committer_date, committer_tz, message, in_main_branch, insertions, deletions, author_name, committer_name, source) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                  ''', (commit.hash, repo_type + "/" + repo_name, commit.author_date, commit.author_timezone, commit.committer_date, commit.committer_timezone, commit.msg, commit.in_main_branch, commit.insertions, commit.deletions, commit.author.name, commit.committer.name, "pydriller"))
                  
                  # Commit the commit
                  conn.commit()

                  try:
                        for file in commit.modified_files:
                              filename = file.new_path
                              if file.change_type.name == 'DELETE':
                                    filename = file.old_path
                              # diff might be too big. If so, field will have value: 'The diff of this file is too large to store it'.
                              # It surpasses the MySQL 'max_allowed_packet' size.  If you are using the mysql client program, Standard MySQL installation has a default value of 1048576 bytes (1MB). I'll keep this as a floor value.
                              diff = file.diff
                              if (sys.getsizeof(file.diff) >> 20) > 0: 
                                    diff = 'The diff of this file is too large to store it'
                              
                              # modified_file table id is composed from repo_name+modified_file+commit_id
                              modified_file_id = hashlib.sha256((repo_name + '_' + file.filename + '_' + commit.hash).encode("utf-8")).hexdigest()

                              cursor.execute('''
                              INSERT IGNORE INTO modified_file (modified_file_id, rfilename, repo_file_id, new_path, old_path, change_type, diff, added_lines, deleted_lines, nloc) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                              ''', (modified_file_id, file.filename, repo_files.get(filename), file.new_path, file.old_path, file.change_type.name, diff, file.added_lines, file.deleted_lines, file.nloc))

                              cursor.execute('''
                              INSERT IGNORE INTO files_in_commit (sha, modified_file_id) VALUES (%s, %s)
                              ''', (commit.hash, modified_file_id))

                  except AttributeError as error:
                        eprint("File searching error. Printing Exception:")
                        eprint(error)
                  except ValueError as ve:
                        eprint("PYDRILLER BUG: There is a bug with parsing of some type of data. Waiting to be fixed...")
                        eprint("Exception:")
                        eprint(ve)
                  
            # os.system("rm -rf " + repo_path)
            shutil.rmtree(path, ignore_errors=False, onerror=onerror)
     
      except GitCommandError:
            eprint("\n")
            eprint("PyDriller Repository clone error (repo: " + repo_name + "): \n", sys.exc_info())
            eprint("\n")
            return


def populate_discussions(cursor, conn, api, repo_name, repo_type):
      """ 
      Importation of discussions information.
      It inserts discussion information into ``discussion``, ``author``, ``conflicting_files_discussion`` and ``discussion_event`` tables.
      
      :param cursor: The MySQL connection cursor to execute operations such as SQL statements.
      :type cursor: `cursor.MySQLCursor`_
      :param conn: A MySQL connector to the database specified in the configuration file. Used to commit changes to fulfill FK restrictions.
      :type  conn: `connection.MySQLConnection`_
      :param api: The huggingface_hub API object pointer.
      :type api: `huggingface_hub.HfApi`_
      :param repo_name: The full name (i.e., "owner/repo_name") of the repository.
      :type repo_name: str
      :param repo_type: The type of the repository (i.e., model, dataset or space) 
      :type repo_type: str
      """

      # If discussions are disabled, it is thrown an HTTPError Exception
      try:
            repo_id = repo_type + 's/' + repo_name
            for discussion in api.get_repo_discussions(repo_id=repo_name, repo_type=repo_type, token=ACCESS_TOKEN):
                  
                  details = api.get_discussion_details(repo_id=repo_name, discussion_num=discussion.num, repo_type=repo_type, token=ACCESS_TOKEN)

                  author_name = details.__dict__.get("author")

                  cursor.execute('''
                  INSERT IGNORE INTO author (username, source) VALUES (%s, %s)
                  ''', (author_name, "hf"))

                  # commit the discussion author
                  conn.commit()

                  # Checking that if there's a merge_commit_oid, the commit is in the database (not the case for private/gated repos)
                  merge_commit_oid = details.__dict__.get("merge_commit_oid")
                  if merge_commit_oid:
                        cursor.execute('''
                        SELECT sha FROM commits WHERE sha=%s
                        ''', [merge_commit_oid])
                        exist = cursor.fetchone()
                        if not exist:
                              merge_commit_oid = None

                  cursor.execute('''
                        INSERT INTO discussion (num, repo_id, author, title, status, created_at, is_pull_request, target_branch, merge_commit_oid, diff, git_reference) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE status = values(status)
                        ''', (details.__dict__.get("num"), repo_id, author_name, details.__dict__.get("title"), details.__dict__.get("status"), details.__dict__.get("created_at"), details.__dict__.get("is_pull_request"), details.__dict__.get("target_branch"), merge_commit_oid, details.__dict__.get("diff"), details.__dict__.get("git_reference")))
                  
                  # commit the discussion
                  conn.commit()

                  # Insert conflicting files if any: list of str
                  # TODO: There's a bug. so we have to check it is a list and not a boolean. Fix it when library is updated
                  if details.conflicting_files and type(details.conflicting_files) == list:
                        for file in details.conflicting_files:
                              repo_file_id = hashlib.sha256((repo_id + '_' + file).encode("utf-8")).hexdigest()
                              cursor.execute('''
                              INSERT IGNORE INTO conflicting_files_discussion (num, repo_id, filename, repo_file_id) VALUES (%s, %s, %s, %s)
                              ''', (details.__dict__.get("num"), repo_id, file, repo_file_id))

                  # Insert events
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
                                    INSERT IGNORE INTO discussion_event (id, repo_id, discussion_num, event_type, created_at, author, content, edited, hidden, full_data) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    ''', (event.__dict__.get("id"), repo_type + 's/' + repo_name, discussion.num,  event.__dict__.get("type"), event.__dict__.get("created_at"), author_name, event.__dict__.get("content"), event.__dict__.get("edited"), event.__dict__.get("hidden"), str(event.__dict__.get("_event"))))
                        elif event.type == "status-change":
                              cursor.execute('''
                                    INSERT IGNORE INTO discussion_event (id, repo_id, discussion_num, event_type, created_at, author, new_status, full_data) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                    ''', (event.__dict__.get("id"), repo_type + 's/' + repo_name, discussion.num, event.__dict__.get("type"), event.__dict__.get("created_at"), author_name, event.__dict__.get("new_status"), str(event.__dict__.get("_event"))))
                        elif event.type == "commit":
                              cursor.execute('''
                                    INSERT IGNORE INTO discussion_event (id, repo_id, discussion_num, event_type, created_at, author,summary, sha, full_data) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    ''', (event.__dict__.get("id"), repo_type + 's/' + repo_name, discussion.num, event.__dict__.get("type"), event.__dict__.get("created_at"), author_name, event.__dict__.get("summary"), event.__dict__.get("oid"), str(event.__dict__.get("_event"))))
                        elif event.type == "title-change":
                              cursor.execute('''
                                    INSERT IGNORE INTO discussion_event (id, repo_id, discussion_num, event_type, created_at, author, old_title, new_title, full_data) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    ''', (event.__dict__.get("id"), repo_type + 's/' + repo_name, discussion.num, event.__dict__.get("type"), event.__dict__.get("created_at"), author_name, event.__dict__.get("old_title"), event.__dict__.get("new_title"), str(event.__dict__.get("_event"))))
      except HfHubHTTPError as e:
            eprint("---------DISCUSSIONS ERROR----------")
            eprint("HuggingFace API returned an error.")
            eprint("Server Message:", e.server_message)
            eprint("Repo: ", repo_name, "\n")
      except RepositoryNotFoundError as ex:
            eprint("---------DISCUSSIONS ERROR----------")
            eprint("The repository to download from cannot be found. This may be because it does not exist, or because it is set to private and you do not have access")
            eprint("Server Message:", ex.server_message)
            eprint("Repo: ", repo_name, "\n")


def populate_space_dependencies(cursor, models, datasets, repo_name):
      """ 
      Importation of space dependency information. 
      It inserts the models and datasets used information into the ``models_in_space`` and ``datasets_in_space`` tables.
      
      :param cursor: The MySQL connection cursor to execute operations such as SQL statements.
      :type cursor: `cursor.MySQLCursor`_
      :param models: A list of model ids.
      :type models: list[str]
      :param datasets: A list of dataset ids.
      :type datasets: list[str]
      :param repo_name: The name of the space repository.
      :type repo_name: str
      """
  
      if models:
            for model in models:
                  cursor.execute('''
                  INSERT IGNORE INTO models_in_space (model_id, space_id) VALUES (%s, %s)
                  ''', ("models/" + model, "spaces/" + repo_name))

      if datasets:
            for dataset in datasets:
                  cursor.execute('''
                  INSERT IGNORE INTO datasets_in_space (dataset_id, space_id) VALUES (%s, %s)
                  ''', ("datasets/" + dataset, "spaces/" + repo_name))


def populate_models(cursor, conn, api, limit_index, limit_date):
      """ 
      Importation of the information of models.
      It retrieves the whole set of models from HFH or, optionally, the first ``limit_index`` elements. 
      It inserts model information into ``repository``, ``model`` and ``author`` tables, and calls the rest of populate methods to fill the remaining tables (not including ``populate_datasets`` and ``populate_spaces``).
      
      :param cursor: The MySQL connection cursor to execute operations such as SQL statements.
      :type cursor: `cursor.MySQLCursor`_
      :param conn: A MySQL connector to the database specified in the configuration file. Used to commit changes to fulfill FK restrictions.
      :type conn: `connection.MySQLConnection <https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlconnection.html>`_
      :param api: The huggingface_hub API object pointer.
      :type api: `huggingface_hub.HfApi <https://huggingface.co/docs/huggingface_hub/v0.16.3/en/package_reference/hf_api#huggingface_hub.HfApi>`_
      :param limit_index: limit_index bound of the slicing of the set of models
      :type limit_index: int
      :param limit_date: Date from which it starts to update the database (e.g., update the database values of the models that have been modified in the last month).
      :type limit_date: datetime
      """

      print("Retrieving full information of models...")
      
      models = api.list_models(full=True, cardData=True, fetch_config=True, sort="lastModified", direction=-1, limit=limit_index, use_auth_token=ACCESS_TOKEN)
      
      print("Starting population into database...")

      updated_models_count = total_models_count = 0

      for model in models:
            model_id = "models/" + model.id
            total_models_count += 1

            # Gather just those with modifications within the limit_date (e.g., last month)
            if model.__dict__.get("lastModified") < limit_date:
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
            
            updated_models_count += 1

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

            # TODO: fields:
            # * CardData not supported. Now it's its own class RepoCard (https://huggingface.co/docs/huggingface_hub/v0.18.0.rc0/en/package_reference/cards#huggingface_hub.RepoCard)
            # * add securityStatus field 
            cursor.execute('''
            INSERT INTO repository (id, name, type, author, sha, last_modified, private, card_data, gated, likes, disabled) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE name = values(name), type = values(type), author = values(author), sha = values(sha), last_modified= values(last_modified), private = values(private), card_data = values(card_data), gated = values(gated), likes = values(likes), disabled = values(disabled) 
            ''', (model_id, model.__dict__.get("id"), "model", model.__dict__.get("author"), model.__dict__.get("sha"), model.__dict__.get("lastModified"), model.__dict__.get("private"), str(model.__dict__.get("cardData")), model.__dict__.get("gated"), model.__dict__.get("likes"), model.__dict__.get("disabled")))

            # Commit to the DB because of the FK constraint
            conn.commit()

            cursor.execute('''
            INSERT INTO model (model_id, pipeline_tag, downloads, library_name, config) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE pipeline_tag = values(pipeline_tag), downloads = values(downloads), library_name = values(library_name), config = values(config)
            ''', (model_id, model.__dict__.get("pipeline_tag"), model.__dict__.get("downloads"), model.__dict__.get("library_name"), config))
            
            
            populate_tags(cursor, conn, model.tags, model.modelId, "models")
            populate_files(cursor, api, model.modelId, "model")
            conn.commit() # Commit of files
            populate_commits(cursor, conn, model.modelId, "models")
            populate_discussions(cursor, conn, api, model.modelId, "model")


      print("Info retrieved! Gathered", total_models_count, "models")
      print(updated_models_count, "models updated!")
      print("Models population finished!")


def populate_datasets(cursor, conn, api, limit_index, limit_date):
      """ 
      Importation of the information of datasets.
      It retrieves the whole set of models from HFH or, optionally, the first ``limit_index`` elements. 
      It inserts dataset information into ``repository``, ``dataset`` and ``author`` tables, and calls the rest of populate methods to fill the remaining tables (not including ``populate_models`` and ``populate_spaces``).
      
      :param cursor: The MySQL connection cursor to execute operations such as SQL statements.
      :type cursor: `cursor.MySQLCursor`_
      :param conn: A MySQL connector to the database specified in the configuration file. Used to commit changes to fulfill FK restrictions.
      :type conn: `connection.MySQLConnection`_
      :param api: The huggingface_hub API object pointer.
      :type api: `huggingface_hub.HfApi`_
      :param limit_index: limit_index bound of the slicing of the set of datasets
      :type limit_index: int
      :param limit_date: Date from which it starts to update the database (e.g., update the database values of the datasets that have been modified in the last month). 
      :type limit_date: datetime
      """

      print("Retrieving information of datasets...")

      # cardData deprecated in v0.14, use just full
      datasets = api.list_datasets(full=True, sort="lastModified", direction=-1, limit=limit_index, use_auth_token=ACCESS_TOKEN)

      print("Starting population into database...")

      updated_dataset_count = total_dataset_count = 0

      for dataset in datasets:

            dataset_id = "datasets/" + dataset.__dict__.get("id")
            total_dataset_count += 1

            if dataset.__dict__.get("lastModified") < limit_date:
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

            updated_dataset_count += 1
            
            # This repo is huge, it is needed a workaround when collecting the commits;
            if dataset.__dict__.get("id") in ["ywchoi/mdpi_sept10", "ACL-OCL/acl-anthology-corpus", "uripper/ProductScreenshots", "gozfarb/ShareGPT_Vicuna_unfiltered","deepsynthbody/deepfake_ecg_full_train_validation_test"]:
                  continue

            if (dataset.author != None):
                  cursor.execute('''
                  INSERT IGNORE INTO author (username, source) VALUES (%s, %s)
                  ''', (dataset.author, "hf_owner"))
                  conn.commit()

            gated =  dataset.__dict__.get("gated")
            if gated in ["manual","auto"]:
                  gated = None # TODO: For now I reported this behavior in GH. I'll keep the `None` until solved. Behavior is legitime. Type of gated should be changed to string: to accept either "Manual"/"Auto", and "Yes"/"No". Maybe use numeric values (0: No, 1: Yes, 2: Auto, 3: Manual)

            cursor.execute('''
            INSERT INTO repository (id, name, type, author, sha, last_modified, private, card_data, gated, likes, disabled) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE name = values(name), type = values(type), author = values(author), sha = values(sha), last_modified= values(last_modified), private = values(private), card_data = values(card_data), gated = values(gated), likes = values(likes), disabled = values(disabled)
            ''', (dataset_id, dataset.__dict__.get("id"), "dataset", dataset.__dict__.get("author"), dataset.__dict__.get("sha"), dataset.__dict__.get("lastModified"), dataset.__dict__.get("private"), str(dataset.__dict__.get("cardData")), gated, dataset.__dict__.get("likes"), dataset.__dict__.get("disabled")))
            conn.commit()

            cursor.execute('''
            INSERT INTO dataset (dataset_id, description, citation, paperswithcode_id, downloads) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE description = values(description), citation = values(citation), paperswithcode_id = values(paperswithcode_id), downloads = values(downloads)
            ''', (dataset_id, dataset.__dict__.get("description"), dataset.__dict__.get("citation"), dataset.__dict__.get("paperswithcode_id"), dataset.__dict__.get("downloads")))         
            

            populate_tags(cursor, conn, dataset.tags, dataset.id, "datasets")
            populate_files(cursor, api, dataset.id, "dataset")                 
            conn.commit()
            populate_commits(cursor, conn, dataset.id, "datasets")
            populate_discussions(cursor, conn, api, dataset.id, "dataset")

      print("Info retrieved! Gathered", total_dataset_count, "datasets.")
      print(updated_dataset_count, "datasets updated!")
      print("Dataset information retrieved!")


def populate_spaces(cursor, conn, api, limit_index, limit_date):
      """ 
      Importation of the information of spaces.
      It retrieves the whole set of models from HFH or, optionally, the first ``limit_index`` elements. 
      It inserts model information into ``repository``, ``space`` and ``author`` tables, and calls the rest of populate methods to fill the remaining tables (not including ``populate_models`` and ``populate_datasets``).
      
      :param cursor: The MySQL connection cursor to execute operations such as SQL statements.
      :type cursor: `cursor.MySQLCursor`_
      :param conn: A MySQL connector to the database specified in the configuration file. Used to commit changes to fulfill FK restrictions.
      :type conn: `connection.MySQLConnection`_
      :param api: The huggingface_hub API object pointer.
      :type api: `huggingface_hub.HfApi`_
      :param limit_index: limit_index bound of the slicing of the set of spaces
      :type limit_index: int
      :param limit_date: Date from which it starts to update the database (e.g., update the database values of the spaces that have been modified in the last month).
      :type limit_date: datetime
      """

      print("Retrieving information of spaces...")
      spaces = api.list_spaces(full=True, sort="lastModified", direction=-1, limit=limit_index, use_auth_token=ACCESS_TOKEN)

      print("Starting population into database...")

      updated_space_count = total_space_count = 0

      for space in spaces:
            
            space_id = "spaces/" + space.__dict__.get("id")
            total_space_count += 1

            if space.__dict__.get("lastModified") < limit_date:
                  cursor.execute('''
                  UPDATE repository 
                  SET likes=%s
                  WHERE id=%s
                  ''', (space.__dict__.get("likes"), space_id))
                  continue
            
            updated_space_count += 1

            # This repo always give error with PyDriller, we'll have to take a look -> in web ERROR in deploying
            if space.__dict__.get("id") in ["mfrashad/ClothingGAN", "mfrashad/CharacterGAN", "fdfdd12345628/Tainan", "patent/demo1"]:
                  continue
            
            if (space.__dict__.get("author") != None):
                  cursor.execute('''
                  INSERT IGNORE INTO author (username, source) VALUES (%s, %s)
                  ''', (space.author, "hf_owner"))
                  conn.commit()

            cursor.execute('''
            INSERT INTO repository (id, name, type, author, sha, last_modified, private, card_data, gated, likes, disabled) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE name = values(name), type = values(type), author = values(author), sha = values(sha), last_modified= values(last_modified), private = values(private), card_data = values(card_data), gated = values(gated), likes = values(likes), disabled = values(disabled)
            ''', (space_id, space.__dict__.get("id"), "space", space.__dict__.get("author"), space.__dict__.get("sha"), space.__dict__.get("lastModified"), space.__dict__.get("private"), str(space.__dict__.get("cardData")), space.__dict__.get("gated"), space.__dict__.get("likes"), space.__dict__.get("disabled")))
            conn.commit()
            
            try:
                  runtime = api.get_space_runtime(space.id, token=ACCESS_TOKEN)
            
                  cursor.execute('''
                  INSERT INTO space (space_id, sdk, stage, hardware, requested_hw, sleep_time, storage, runtime_raw) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE sdk = values(sdk), stage = values(stage), hardware = values(hardware), requested_hw = values(requested_hw), sleep_time = values(sleep_time), storage = values(storage), runtime_raw = values(runtime_raw)
                  ''', (space_id, space.__dict__.get("sdk"), runtime.__dict__.get("stage"), runtime.__dict__.get("hardware"), runtime.__dict__.get("requested_hw"), runtime.__dict__.get("sleep_time"), runtime.__dict__.get("storage"), runtime.__dict__.get("runtime_raw")))
                  conn.commit()
            except RepositoryNotFoundError as err:
                  eprint("------GET SPACE RUNTIME ERROR-------")
                  eprint("Error:", str(err))
                  eprint("Hint: Repository might be deleted while the script was running.\n")

              
            
            populate_space_dependencies(cursor, space.models, space.datasets, space.id)
            populate_tags(cursor, conn, space.tags, space.id, "spaces")
            populate_files(cursor, api, space.id, "space")
            conn.commit()
            populate_commits(cursor, conn, space.id, "spaces")
            populate_discussions(cursor, conn, api, space.id, "space")

      print("Info retrieved! Gathered", total_space_count, "spaces.")
      print(updated_space_count, "spaces updated!")
      print("Spaces information retrieved!")



def main(argv):
      """ Main method. Called when invoking the script. """

      # The execution time is measured and dumped to stdout
      start_time = time.time()

      if len(argv) == 0:
            eprint("USAGE: python databaseImport.py -c")
            eprint("       python databaseImport.py -t {model|dataset|space|all} [-i limit_index] [-s]")
            sys.exit(1)

      try:
            opts, args = getopt.getopt(argv, "t:i:cs", []) # TODO: Refactor. Use argparse
      except getopt.GetoptError:
            eprint("USAGE: python databaseImport.py -c")
            eprint("       python databaseImport.py -t [model|dataset|space|all] [-i limit_index] [-s]")
            sys.exit(1)
      
      config = read_config()

      try:
            ACCESS_TOKEN = config["hfh_token"]
      except KeyError as ke:
            eprint("Missing token in hfc.config file. Please add token in 'hfh_token' field.")
            sys.exit(1)


      validate_token(ACCESS_TOKEN)
      
      
      # Monthly recovery. NOT BY DEFAULT.
      # Default date (Jan 1st 1970 - UNIX epoch time)
      limit_date = pytz.UTC.localize(datetime.fromtimestamp(0))
      try:
            last_n_months = config["last_n_months"]
            if last_n_months == -1:
                  print("`last_n_months` parameter of hfc.config is at default value. Retrieving all information!")
            else:
                  limit_date = pytz.UTC.localize((datetime.now() - dateutil.relativedelta.relativedelta(months=last_n_months)).replace(day=1, hour=0, minute=0, second=0, microsecond=0))
      except KeyError as ke:
            print("Missing `n_last_month` parameter in hfc.config file. Retrieving all information!")


      limit_index = None
      repo_type = ""
      skip = False

      conn, database = create_connection_mysql()

      c = conn.cursor()
      print("Connection to DB done!")
      
      for opt, arg in opts:
            if opt in ("-t"):
                  repo_type = arg
            if opt in ("-i"):
                  limit_index = int(arg)
            if opt in ("-c"):
                  print("Flag -c detected, creating the database schema...")
                  create_schema_mysql(c)
                  print("Database schema created!")
                  print("If you want to start importing repositories, launch again this script without -c flag!")
                  sys.exit(0)
            if opt in ("-s"):
                  skip = True
                  print("Flag -s detected. Skipping repos with parameters of hfc.config.")
                  global NUM_COMMITS
                  NUM_COMMITS = config['max_num_commits']
                  global NUM_FILES
                  NUM_FILES = config['max_num_files']
      
      
      # Settings to avoid formatting error when inserting text
      c.execute("SET NAMES utf8mb4")
      c.execute("SET CHARACTER SET utf8mb4")
      c.execute("SET character_set_connection=utf8mb4")
      c.execute("SET character_set_server=utf8mb4")
      query = """ALTER DATABASE {} CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci;""".format(config["database"])
      c.execute(query)

      
      tables_exist = check_database_schema(c, database)

      if not tables_exist:
            eprint("There are some (or all) tables required missing in the target database.\nThis script will delete any existing table of the HFC schema (it will leave any other existing table of the database) and create the full schema.\nCreating database schema...")
            create_schema_mysql(c)
            print("Database schema created!")
      
      api = HfApi()

      # Import by type.
      if repo_type == "dataset":
            populate_datasets(c, conn, api, limit_index, limit_date)
      elif repo_type == "space":
            populate_spaces(c, conn, api, limit_index, limit_date)
      elif repo_type == "model":
            populate_models(c, conn, api, limit_index, limit_date)
      elif repo_type=="all":
            populate_datasets(c, conn, api, limit_index, limit_date)
            populate_spaces(c, conn, api, limit_index, limit_date)
            populate_models(c, conn, api, limit_index, limit_date)
      else:
            print("USAGE: python databaseImport.py -c")
            print("       python databaseImport.py -t [model|dataset|space|all] [-i limit_index] [-s]")
            sys.exit(0)

      # Save (commit) the changes
      conn.commit()
      
      # We can also close the connection if we are done with it.
      # Just be sure any changes have been committed or they will be lost.
      c.close()
      conn.close()

      
      if skip:
            print("SKIPPED REPOS: ", SKIPPED_REPOS)

      # Printing execution time...
      exec_time = time.time() - start_time
      print("\nExecution time:")
      print("--- %s seconds ---" % (exec_time))
      print("--- %s minutes ---" % (round(exec_time/60,2)))
      print("--- %s hours ---" % (round(exec_time/3600,2)))

if __name__ == "__main__":
    main(sys.argv[1:])