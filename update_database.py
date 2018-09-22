from arctic import Arctic, CHUNK_STORE
from arctic.exceptions import NoDataFoundException, LibraryNotFoundException
import pandas as pd
from tqdm import tqdm
import rqdatac as rq

from datetime import date

arctic = Arctic('localhost')

# TODO: replace print with logging
# TODO: refactor update and init functions
# TODO: write test before refactor


def init_libraries():
    arctic.initialize_library('basedata')
    arctic.initialize_library('daily', lib_type=CHUNK_STORE)
    arctic.initialize_library('minute', lib_type=CHUNK_STORE)


def update_basedata():
    basedata = arctic['basedata']
    stocks = rq.all_instruments('CS')
    idx = rq.all_instruments('INDX')

    try:
        updated = basedata.list_versions()[0]['date'].date()
        if updated == date.today():
            print('Already updated today')
            return
    except:
        pass

    documents = {'stocks': stocks, 'indexes': idx}

    for label, data in documents.items():
        basedata.write(label, data)


def all_sid():
    basedata = arctic['basedata']
    stocks = basedata.read('stocks').data['order_book_id']
    idx = basedata.read('indexes').data['order_book_id']
    return pd.concat([stocks, idx])


def init_minute_lib():
    try:
        minute_lib = arctic['minute']
        print('The minute library is already exists.')
        return
    except LibraryNotFoundException:
        arctic.initialize_library('minute', lib_type=CHUNK_STORE)

    minute_lib = arctic['minute']
    start_date = '2018-01-01'

    for sid in tqdm(all_sid()):
        try:
            df = rq.get_price(
                sid,
                start_date=start_date,
                end_date=date.today(),
                frequency='1m',
                adjust_type='post')
            df.index.name = 'date'
            if len(df) > 0:
                minute_lib.write(sid, df, chunk_size='D')
        except Exception as e:
            # TODO: add logger later
            print(f'{sid}: {str(e)}')


def update_minute_lib():
    minute_lib = arctic['minute']
    last_index = minute_lib.read(
        '000001.XSHG', columns=['close'], chunk_range=('2018-09-20',
                                                       None)).index[-1]
    start_date = rq.get_next_trading_date(last_index)

    for sid in tqdm(all_sid()):
        try:
            df = rq.get_price(
                sid,
                start_date=start_date,
                end_date=date.today(),
                frequency='1m',
                adjust_type='post')
            df.index.name = 'date'
            if len(df) > 0:
                minute_lib.update(
                    sid, df, chunk_range=(start_date, None), upsert=True)
        except Exception as e:
            # TODO: add logger later
            print(f'{sid}: {str(e)}')


def main():
    rq.init()
    # init_libraries()
    update_basedata()
    # init_minute_lib()


if __name__ == '__main__':
    main()
