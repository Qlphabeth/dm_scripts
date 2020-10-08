import pandas as pd


def indicatoren_VSV_parser(filename):
    data = pd.read_excel(filename, header=[0, 1, 2], sheetname='VSV')
    data_dict = data.to_dict('index')
    return data_dict


if __name__ == '__main__':
    filename = '01-mbo-indicatoren-per-instelling-2019.xls'
    indicatoren_VSV_parser(filename)
