import pandas as pd


def indicatoren_parser(filename):
    """
    Enter a xls or xlsx file and get a dictionary back, with the Brinnr as key and a dict as value
    The year results and diploma results are in a dict again, separated in Inst. and Vgl Grp,
    and therein another dict with the years and results in percentages or as nan
    """
    data_dict = {}
    data = pd.read_excel(filename, header=[0, 1, 2], index_col=None)
    keys = []
    for row in data:
        keys.append(row)

    for i in range(len(keys)):
        j = list(keys[i])
        for elem in keys[i]:
            if 'Unnamed' in elem:
                j.remove(elem)
        keys[i] = tuple(j)

    for row in data.values:
        data_dict[row[0]] = {}
        cur_dict = data_dict[row[0]]
        adres = []
        for i, elem in enumerate(row):
            if len(keys[i]) == 1:
                cur_dict[keys[i][0]] = elem
            elif len(keys[i]) == 2:
                adres.append(elem)
            elif len(keys[i]) == 3:
                if keys[i][0] not in cur_dict.keys():
                    cur_dict[keys[i][0]] = {}
                if keys[i][2] not in cur_dict[keys[i][0]]:
                    cur_dict[keys[i][0]][keys[i][2]] = {}
                if elem < 1 or elem == 'nan':
                    pass
                else:
                    elem = elem / 100
                cur_dict[keys[i][0]][keys[i][2]][keys[i][1]] = elem

        cur_dict['Adres'] = '%s %s, %s' % (adres[0], adres[1], adres[2])
        cur_dict['Plaats'] = adres[3]
        cur_dict['Vgl Groep'] = adres[4]

    return data_dict


if __name__ == '__main__':
    data = indicatoren_parser('01-mbo-indicatoren-per-instelling-2019.xls')
    for row in data:
        print(data[row])
