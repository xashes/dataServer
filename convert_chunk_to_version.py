from arctic import Arctic

arctic = Arctic('localhost')
arctic.delete_library('day')
arctic.initialize_library('day')

day_lib = arctic['day']
daily_lib = arctic['daily']

from tqdm import tqdm

all_sid = daily_lib.list_symbols()

for s in tqdm(all_sid):
    df = daily_lib.read(s)
    day_lib.write(s, df)
