import pandas as pd


def JOB_parser(filename):
    data_dict = {}
    data = pd.read_excel(filename, header=0, sheet_name='JOB-monitor 2020')
    keys = []
    for row in data:
        keys.append(row)

    for i in range(len(data.values)):
        cur_dict = {}
        cur_val = data.values[i]
        for j in range(len(data.values[i])):
            cur_dict[keys[j]] = cur_val[j]
        data_dict[i] = cur_dict
    return data_dict


if __name__ == '__main__':
    JOB_parser('20200702_JOB-monitor 2020 databestand_v3.0.xlsx')
