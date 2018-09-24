from concurrent.futures import ThreadPoolExecutor, wait
from datetime import date

from arctic import Arctic
from tqdm import tqdm

arctic = Arctic('localhost')
# arctic.delete_library('day')
# arctic.initialize_library('day')

day_lib = arctic['day']
daily_lib = arctic['daily']
minute_chunk = arctic['minute']


def convert_daily_to_version():
    all_sid = daily_lib.list_symbols()
    for s in tqdm(all_sid):
        df = daily_lib.read(s)
        day_lib.write(s, df)


def convert_minute_to_version():
    arctic.delete_library('minute1')
    arctic.initialize_library('minute1')
    minute_version = arctic['minute1']

    def mv_chunk_to_version(sid):
        df = minute_chunk.read(sid)
        minute_version.write(sid, df)

    # TODO: try multiThread
    all_sid = minute_chunk.list_symbols()
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(mv_chunk_to_version, s) for s in tqdm(all_sid)
        ]
        wait(futures)
    print('convert complete')
    minute_version.snapshot(str(date.today()))


def main():
    convert_minute_to_version()


if __name__ == '__main__':
    main()
