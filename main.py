from sql_writer import write_data
from indicatoren_parser import indicatoren_parser
from sys import argv
import os


error_dict = {
    0: "Successful",
    1: "File does not exist in path",
    2: "Database does not exist in path",
    98: "Filetype does not exist. Options: indicatoren or job",
    99: "Usage: main.py filepath type_of_file (database_path)"
}


def main(argc):
    """

    :param argc:
    :return:
    """
    # Check if file exists
    if not os.path.isfile(argv[1]):
        return 1

    if argc == 3:
        result_in = write_data(argv[1], file_type=argv[2])
    else:
        result_in = write_data(argv[1], file_type=argv[2], db_path=argv[3])

    return result_in

# Usage: main.py filepath type_of_file (database_path)
if __name__ == "__main__":
    if len(argv) == 3 or len(argv) == 4:
        result = main(len(argv))
    else:
        result = 99
    print(error_dict.get(result))
