import pandas as pd


def JOB_parser(filename):
    data = pd.read_excel(filename, header=0, sheet_name='JOB-monitor 2020')
    dict = data.to_dict('index')
    return dict


if __name__ == '__main__':
    filename = '20200702_JOB-monitor 2020 databestand_v3.0.xlsx'
    JOB_parser(filename)
