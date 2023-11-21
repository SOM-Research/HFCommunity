# HFCommunity-extractor

This folder contains the Dataset Extractor script and all the necessary files to execute it.
Refer to the [docs](https://github.com/SOM-Research/HFCommunity/docs/usage.html) for further information.

* `hfc.config` is the configuration file. 

    * `host` and `port` are the host and port to be used to connect to the database. **Mandatory**
    * `user` and `pass` are the user and password to access to the database. **Mandatory**
    * `database` is the database schema to be used **Mandatory**
    * `last_n_months` is the number of months from which fully update the repositories, it is an integer greater or equal than 1 *(optional)*
    * `max_num_commits` sets the threshold for commits (i.e., repositories with more commits will not be retrieved) *(optional)*
    * `max_num_files` sets the threshold for commfiles *(optional)*

* `requirements.txt` contains the dependencies of the script.