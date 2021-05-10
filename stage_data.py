import iso4217

ccy_dict = iso4217.raw_table

max_num = len(ccy_dict)

for i in range(max_num):
    ccy = (list(ccy_dict_v)[i]).get('Ccy')
    print(ccy)

for i in range(max_num):
    ccy_nm = (list(ccy_dict_v)[i]).get('CcyNm')
    print(ccy_nm)