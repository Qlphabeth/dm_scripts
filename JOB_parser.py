import pandas as pd


def JOB_parser(filename):
    data_dict = {}
    data = pd.read_excel(filename, header=0, sheet_name='JOB-monitor 2020')
    keys = []
    for row in data:
        keys.append(row)

    for i in range(len(data.values)):
        data_dict[i] = {}
        for j in range(len(data.values[i])):
            data_dict[i][keys[j]] = data.values[i][j]
    return data_dict


if __name__ == '__main__':
    JOB_parser('20200702_JOB-monitor 2020 databestand_v3.0.xlsx')
