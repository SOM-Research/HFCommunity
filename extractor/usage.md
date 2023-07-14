# HOW TO USE HFC-Extractor

`python databaseImport.py -t type -l lower -u upper -o`

When executing the importer we have to write down:

* the type of repository
* the interval of repos
* Whether we are doing an update or not (This flag, -o, is not accompanied by any attribute)

FLAGS:
* -t -> which repository type we are gathering
* -l -> lower margin
* -u -> upper margin
* -o -> to update