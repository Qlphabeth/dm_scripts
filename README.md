# dm_scripts
scripts for data management INF
This is for a project by Wageningen University & Research (WUR) to find a client with a database problem and help solve their problem by making a sturdy database for them, including some SQL queries and automatic data addition as soon new data is available.
This project is coordinated by chairgroup INF, for the course INF-21306 Data Management, and executed by the following students:
* Jinwen Lu
* Max Luppes
* Matthijs Pon
* Ivar Scholten
* Quinten van der Zanden

Current files:
* script.py - used to download the required data from the government. (Works only if you have a safe connection with the server, otherwise you have to download them yourself.)
* indicatoren_parser.py - used to parse the data from the government, returns a dict with the information.
* indicatoren_VSV_parser.py - same as above, but for the second sheet in the file.
