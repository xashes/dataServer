from datetime import date

import pytest
import rqdatac as rq

rq.init()


def test_get_next_trading_date():
    assert rq.get_next_trading_date('2018-09-21') == date(2018, 9, 25)
    assert rq.get_next_trading_date('2018-09-20') == date(2018, 9, 21)
