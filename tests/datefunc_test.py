from statnett_usecase.datefunc import date_diff_mins,local_time
import arrow



def test_date_diff_mins():
    date1 = arrow.get('2023-05-24T10:00',tzinfo='Europe/Copenhagen')
    date1_plus_5 = arrow.get('2023-05-24T10:05',tzinfo='Europe/Copenhagen')
    date1_plus_test = date_diff_mins(date1,5)
    assert date1_plus_5 == date1_plus_test


