import json


with open('data/purchase_log.txt', 'r') as f:
    data = f.read()
    data_json = json.loads('[' + data.replace('\n', ',')[:-1] + ']')

data_transform = dict()
for line in data.split('\n'):
    if not line:
        continue
    line_ = (
        line
        .replace('{', '')
        .replace('}', '')
        .replace('"', '')
        .split(',')
    )
    user_id = line_[0].split(':')[-1].strip()
    category = line_[1].split(':')[-1].strip()
    data_transform[user_id] = data_transform.get(user_id, []) + [category]

with open('data/visit_log.csv', 'r') as f_in, \
        open('data/funnel.csv', 'w') as f_out:
    f_in.readline()
    f_out.write('user_id,source,category\n')
    for line in f_in:
        user_id = line.split(',')[0]
        purchase = data_transform.get(user_id)
        if purchase:
            f_out.write(f'{line[:-1]},{"|".join(purchase)}\n')


# funnel.csv:
# user_id,source,category
# 1840e0b9d4,other,Продукты
# 4e4f90fcfb,context,Электроника
# afea8d72fc,other,Электроника
# 2824221f38,email,Продукты
# ...
# 6f24969f4e,other,Электроника|Строительство и ремонт
# ...
