select id, name, age, amount,
user_info['sex'] as sex,
user_info['address'] as address,
user_info['phone'] as phone
from {table}
