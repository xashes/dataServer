from arctic import Arctic, CHUNK_STORE
from arctic.exceptions import LibraryNotFoundException
import pandas as pd
from tqdm import tqdm
import rqdatac as rq

from datetime import date, time

arctic = Arctic('localhost')

# TODO: replace print with logging
# TODO: refactor update and init functions
# TODO: write test before refactor


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


def init_minute1_lib():
    try:
        minute1_lib = arctic['minute1']
        print('The minute1 library is already exists.')
        return
    except LibraryNotFoundException:
        arctic.initialize_library('minute1')

    minute1_lib = arctic['minute1']
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
                minute1_lib.write(sid, df)
        except Exception as e:
            # TODO: add logger later
            print(f'{sid}: {str(e)}')
    minute1_lib.snapshot(str(date.today()))


def update_minute1_lib():
    minute1_lib = arctic['minute1']
    last_index = minute1_lib.read(
        '000001.XSHG').data.index[-1]

    if last_index.time() == time(15):
        start_date = rq.get_next_trading_date(last_index)

        if not rq.get_trading_dates(start_date, date.today()):
            print('DB is already up to date.')
            return

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
                    minute1_lib.append(
                        sid, df, upsert=True)
            except Exception as e:
                # TODO: add logger later
                print(f'{sid}: {str(e)}')
    else:
        # read previous version, and get last index to compute start date
        previous_version = minute1_lib.list_versions('000001.XSHG')[1]
        last_index = minute1_lib.read('000001.XSHG', as_of=previous_version['version']).data.index[-1]
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
                pre_version_number = minute1_lib.list_versions(sid)[1]['version']
                minute1_lib.restore_version(sid, pre_version_number)
                if len(df) > 0:
                    minute1_lib.append(
                        sid, df, upsert=True)
            except Exception as e:
                # TODO: add logger later
                print(f'{sid}: {str(e)}')


def init_day_lib():
    try:
        day_lib = arctic['day']
        print('The day library is already exists.')
        return
    except LibraryNotFoundException:
        arctic.initialize_library('day')

    day_lib = arctic['day']
    start_date = '2000-01-04'

    for sid in tqdm(all_sid()):
        try:
            df = rq.get_price(
                sid,
                start_date=start_date,
                end_date=date.today(),
                frequency='1d',
                adjust_type='post')
            df.index.name = 'date'
            if len(df) > 0:
                day_lib.write(sid, df)
        except Exception as e:
            # TODO: add logger later
            print(f'{sid}: {str(e)}')


def update_day_lib():
    day_lib = arctic['day']
    last_index = day_lib.read(
        '000001.XSHG').data.index[-1]
    start_date = rq.get_next_trading_date(last_index)
    if not rq.get_trading_dates(start_date, date.today()):
        print('DB is already up to date.')
        return

    for sid in tqdm(all_sid()):
        try:
            df = rq.get_price(
                sid,
                start_date=start_date,
                end_date=date.today(),
                frequency='1d',
                adjust_type='post')
            df.index.name = 'date'
            if len(df) > 0:
                day_lib.append(
                    sid, df, upsert=True)
        except Exception as e:
            # TODO: add logger later
            print(f'{sid}: {str(e)}')


def main():
    rq.init()
    update_basedata()
    update_day_lib()
    update_minute1_lib()


if __name__ == '__main__':
    main()
