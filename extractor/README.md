# HFCommunity-extractor

This folder contains the Dataset Extractor script and all the necessary files to execute it.
Refer to the ![docs](https://som-research.github.io/HFCommunity/docs/usage.html) for further information.

* `hfc.config` is the configuration file.
    * Parameters `host`, `port`, `user`, `pass`, `database` and `hfh_token` are mandatory. `last_n_months` parameter, which has to be an integer greater or equal than 1, and `max_num_commits` and `max_num_files` are optional (refer to the ![docs](https://github.com/SOM-Research/HFCommunity/docs/usage.html) for further information).
* `requirements.txt` contains the dependencies of the script.
