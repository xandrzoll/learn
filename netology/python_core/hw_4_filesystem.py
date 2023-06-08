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
        f_out.write(f'{line[:-1]},"{data_transform.get(user_id, "None")}"\n')


# funnel.csv:
# user_id,source,category
# 6450655ae8,other,"None"
# ...
# b18d58560b,email,"None"
# ed79586589,other,"None"
# 1840e0b9d4,other,"['Продукты']"