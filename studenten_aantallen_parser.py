import pandas as pd


def aantallen_parser(filename):
    data = pd.read_excel(filename, header=0, sheet_name=None)
    data_d = {}
    for sheet in data:
        dd = data[sheet].to_dict('index')
        data_d[sheet] = dd
    return data_d


if __name__ == '__main__':
    filename = 'studentaantallen geanonimiseerd wur.xlsx'
    data = aantallen_parser(filename)
