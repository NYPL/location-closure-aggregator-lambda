import json
import lambda_function
import os
import pandas as pd
import pytest

from datetime import time
from pandas.testing import assert_frame_equal


def convert_df_types(input_df):
    return input_df.astype({
        'extended_closing': 'bool',
        'alert_start': 'datetime64[ns, UTC]',
        'alert_end': 'datetime64[ns, UTC]',
        'polling_datetime': 'datetime64[ns, UTC]'})


def get_polling_times(start, end):
    return ['2023-01-01 {:02d}:01:23-05'.format(i) for i in range(start, end)]


_BASE_CLOSURES_DF = pd.DataFrame({
    'drupal_location_id': ['aa'],
    'name': ['Library A'],
    'alert_id': ['1'],
    'closed_for': ['Lib A is closed'],
    'is_extended_closure': [False],
    'is_full_day': [False],
    'closure_date': ['2023-01-01'],
    'closure_start': ['11:00:00'],
    'closure_end': ['14:00:00']
})

_BASE_ALERTS_DF = convert_df_types(pd.DataFrame({
    'drupal_location_id': [None]*15,
    'name': [None]*15,
    'alert_id': [None]*15,
    'closed_for': [None]*15,
    'extended_closing': [None]*15,
    'alert_start': [None]*15,
    'alert_end': [None]*15,
    'regular_open': [None]*15,
    'regular_close': [None]*15,
    'polling_datetime': get_polling_times(6, 21)
}))


class TestLambdaFunction:

    @classmethod
    def setup_class(cls):
        os.environ['REDSHIFT_DB_HOST'] = 'test_redshift_host'
        os.environ['REDSHIFT_DB_NAME'] = 'test_redshift_db'
        os.environ['REDSHIFT_DB_USER'] = 'test_redshift_user'
        os.environ['REDSHIFT_DB_PASSWORD'] = 'test_redshift_password'
        os.environ['REDSHIFT_HOURS_TABLE'] = 'test_hours_table'
        os.environ[
            'REDSHIFT_CLOSURE_ALERTS_TABLE'] = 'test_closure_alerts_table'
        os.environ['REDSHIFT_CLOSURES_TABLE'] = 'test_closures_table'

    @classmethod
    def teardown_class(cls):
        del os.environ['REDSHIFT_DB_HOST']
        del os.environ['REDSHIFT_DB_NAME']
        del os.environ['REDSHIFT_DB_USER']
        del os.environ['REDSHIFT_DB_PASSWORD']
        del os.environ['REDSHIFT_HOURS_TABLE']
        del os.environ['REDSHIFT_CLOSURE_ALERTS_TABLE']
        del os.environ['REDSHIFT_CLOSURES_TABLE']

    @ pytest.fixture
    def test_instance(self, mocker):
        mocker.patch('lambda_function.create_log')
        mocker.patch('lambda_function.build_get_alerts_query',
                     return_value='REDSHIFT ALERTS QUERY')

    @ pytest.fixture
    def mock_kms_client(self, mocker):
        mock_kms_client = mocker.MagicMock()
        mock_kms_client.decrypt.side_effect = [
            'decrypted_host', 'decrypted_user', 'decrypted_password']
        mocker.patch('lambda_function.KmsClient', return_value=mock_kms_client)
        return mock_kms_client

    def test_lambda_handler(self, test_instance, mock_kms_client, mocker):
        mock_redshift_client = mocker.MagicMock()
        mock_redshift_cursor = mocker.MagicMock()
        mock_redshift_client.conn.cursor.return_value = mock_redshift_cursor
        mock_redshift_client.execute_query.return_value = _BASE_ALERTS_DF
        mocker.patch('lambda_function.RedshiftClient',
                     return_value=mock_redshift_client)

        mocker.patch('lambda_function.get_closures',
                     return_value=_BASE_CLOSURES_DF)

        assert lambda_function.lambda_handler(None, None) == {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Job ran successfully."
            })
        }

        mock_kms_client.decrypt.assert_has_calls([
            mocker.call('test_redshift_host'),
            mocker.call('test_redshift_user'),
            mocker.call('test_redshift_password')])
        mock_kms_client.close.assert_called_once()

        mock_redshift_client.connect.assert_called_once()
        mock_redshift_client.execute_query.assert_called_once_with(
            'REDSHIFT ALERTS QUERY')
        mock_redshift_cursor.write_dataframe.assert_called_once_with(
            _BASE_CLOSURES_DF, 'test_closures_table')
        mock_redshift_cursor.execute.assert_has_calls([
            mocker.call('BEGIN TRANSACTION;'),
            mocker.call('DELETE FROM test_closure_alerts_table;'),
            mocker.call('END TRANSACTION;'),
        ])
        mock_redshift_client.conn.commit.assert_called_once()
        mock_redshift_client.close_connection.assert_called_once()

    def test_temp_closure(self, test_instance):
        _ALERTS_DF = pd.DataFrame({
            'drupal_location_id': ['aa']*3,
            'name': ['Library A']*3,
            'alert_id': ['1']*3,
            'closed_for': ['Lib A is closed']*3,
            'extended_closing': [False]*3,
            'alert_start': ['2023-01-01 11:00:00-05']*3,
            'alert_end': ['2023-01-01 14:00:00-05']*3,
            'polling_datetime': get_polling_times(11, 14),
            'regular_open': [time(9)]*3,
            'regular_close': [time(17)]*3})
        _FULL_DF = convert_df_types(
            pd.concat([_BASE_ALERTS_DF, _ALERTS_DF], ignore_index=True))

        assert_frame_equal(
            lambda_function.get_closures(_FULL_DF), _BASE_CLOSURES_DF,
            check_like=True)

    def test_extended_closure(self, test_instance):
        _ALERTS_DF = pd.DataFrame({
            'drupal_location_id': ['bb']*15,
            'name': ['Library B']*15,
            'alert_id': ['2']*15,
            'closed_for': ['Lib B is closed']*15,
            'extended_closing': [True]*15,
            'alert_start': ['2022-06-01 00:00:00-04']*15,
            'alert_end': ['2024-06-01 00:00:00-04']*15,
            'polling_datetime': get_polling_times(6, 21),
            'regular_open': [None]*15,
            'regular_close': [None]*15})
        _FULL_DF = convert_df_types(
            pd.concat([_BASE_ALERTS_DF, _ALERTS_DF], ignore_index=True))

        _CLOSURES_DF = pd.DataFrame({
            'drupal_location_id': ['bb'],
            'name': ['Library B'],
            'alert_id': ['2'],
            'closed_for': ['Lib B is closed'],
            'is_extended_closure': [True],
            'is_full_day': [True],
            'closure_date': ['2023-01-01'],
            'closure_start': [None],
            'closure_end': [None]
        })

        assert_frame_equal(
            lambda_function.get_closures(_FULL_DF), _CLOSURES_DF,
            check_like=True)

    def test_clamped_closures(self, test_instance):
        _ALERTS_DF = pd.DataFrame({
            'drupal_location_id': ['cc']*6 + ['dd']*6,
            'name': ['Library C']*6 + ['Library D']*6,
            'alert_id': ['3']*6 + ['4']*6,
            'closed_for': ['Lib C is closed']*6 + ['Lib D is closed']*6,
            'extended_closing': [False]*12,
            'alert_start':
                ['2023-01-01 06:30:00-05']*6 + ['2023-01-01 15:30-05']*6,
            'alert_end':
                ['2023-01-01 12:30:00-05']*6 + ['2023-01-01 21:30:00-05']*6,
            'polling_datetime':
                get_polling_times(7, 13) + get_polling_times(16, 22),
            'regular_open': [time(9)]*12,
            'regular_close': [time(17)]*12})
        _FULL_DF = convert_df_types(
            pd.concat([_BASE_ALERTS_DF, _ALERTS_DF], ignore_index=True))

        _CLOSURES_DF = pd.DataFrame({
            'drupal_location_id': ['cc', 'dd'],
            'name': ['Library C', 'Library D'],
            'alert_id': ['3', '4'],
            'closed_for': ['Lib C is closed', 'Lib D is closed'],
            'is_extended_closure': [False, False],
            'is_full_day': [False, False],
            'closure_date': ['2023-01-01', '2023-01-01'],
            'closure_start': ['09:00:00', '15:30:00'],
            'closure_end': ['12:30:00', '17:00:00']
        })

        assert_frame_equal(
            lambda_function.get_closures(_FULL_DF), _CLOSURES_DF,
            check_like=True)

    def test_full_day_closure(self, test_instance):
        _ALERTS_DF = pd.DataFrame({
            'drupal_location_id': ['ee']*10,
            'name': ['Library E']*10,
            'alert_id': ['5']*10,
            'closed_for': ['Lib E is closed']*10,
            'extended_closing': [False]*10,
            'alert_start': ['2023-01-01 08:00:00-05']*10,
            'alert_end': ['2023-01-01 18:00:00-05']*10,
            'polling_datetime': get_polling_times(8, 18),
            'regular_open': [time(9)]*10,
            'regular_close': [time(17)]*10})
        _FULL_DF = convert_df_types(
            pd.concat([_BASE_ALERTS_DF, _ALERTS_DF], ignore_index=True))

        _CLOSURES_DF = pd.DataFrame({
            'drupal_location_id': ['ee'],
            'name': ['Library E'],
            'alert_id': ['5'],
            'closed_for': ['Lib E is closed'],
            'is_extended_closure': [False],
            'is_full_day': [True],
            'closure_date': ['2023-01-01'],
            'closure_start': ['09:00:00'],
            'closure_end': ['17:00:00']
        })

        assert_frame_equal(
            lambda_function.get_closures(_FULL_DF), _CLOSURES_DF,
            check_like=True)

    def test_out_of_bounds_closure(self, test_instance):
        _ALERTS_DF = pd.DataFrame({
            'drupal_location_id': ['ff']*2,
            'name': ['Library F']*2,
            'alert_id': ['6']*2,
            'closed_for': ['Lib F is closed']*2,
            'extended_closing': [False]*2,
            'alert_start': ['2023-01-01 06:00:00-05']*2,
            'alert_end': ['2023-01-01 08:00:00-05']*2,
            'polling_datetime': get_polling_times(6, 8),
            'regular_open': [time(9)]*2,
            'regular_close': [time(17)]*2})
        _FULL_DF = convert_df_types(
            pd.concat([_BASE_ALERTS_DF, _ALERTS_DF], ignore_index=True))

        assert lambda_function.get_closures(_FULL_DF) is None

    def test_unavailable_hours_closure(self, test_instance):
        _ALERTS_DF = pd.DataFrame({
            'drupal_location_id': ['gg']*3,
            'name': ['Library G']*3,
            'alert_id': ['7']*3,
            'closed_for': ['Lib G is closed']*3,
            'extended_closing': [False]*3,
            'alert_start': ['2023-01-01 10:00:00-05']*3,
            'alert_end': ['2023-01-01 13:00:00-05']*3,
            'polling_datetime': get_polling_times(10, 13),
            'regular_open': [None]*3,
            'regular_close': [None]*3})
        _FULL_DF = convert_df_types(
            pd.concat([_BASE_ALERTS_DF, _ALERTS_DF], ignore_index=True))

        _CLOSURES_DF = pd.DataFrame({
            'drupal_location_id': ['gg'],
            'name': ['Library G'],
            'alert_id': ['7'],
            'closed_for': ['Lib G is closed'],
            'is_extended_closure': [False],
            'is_full_day': [True],
            'closure_date': ['2023-01-01'],
            'closure_start': [None],
            'closure_end': [None]
        })

        assert_frame_equal(
            lambda_function.get_closures(_FULL_DF), _CLOSURES_DF,
            check_like=True)

    def test_modified_closure(self, test_instance):
        _ALERTS_DF = pd.DataFrame({
            'drupal_location_id': ['hh']*4,
            'name': ['Library H']*4,
            'alert_id': ['8']*4,
            'closed_for': ['orig closed_for']*2 + ['new closed_for']*2,
            'extended_closing': [False]*4,
            'alert_start':
                ['2023-01-01 09:00:00-05']*2 + ['2023-01-01 10:00:00-05']*2,
            'alert_end':
                ['2023-01-01 11:00:00-05']*2 + ['2023-01-01 13:00:00-05']*2,
            'polling_datetime': get_polling_times(9, 13),
            'regular_open': [time(9)]*4,
            'regular_close': [time(17)]*4})
        _FULL_DF = convert_df_types(
            pd.concat([_BASE_ALERTS_DF, _ALERTS_DF], ignore_index=True))

        _CLOSURES_DF = pd.DataFrame({
            'drupal_location_id': ['hh'],
            'name': ['Library H'],
            'alert_id': ['8'],
            'closed_for': ['new closed_for'],
            'is_extended_closure': [False],
            'is_full_day': [False],
            'closure_date': ['2023-01-01'],
            'closure_start': ['10:00:00'],
            'closure_end': ['13:00:00']
        })

        assert_frame_equal(
            lambda_function.get_closures(_FULL_DF), _CLOSURES_DF,
            check_like=True)

    def test_deleted_closure(self, test_instance):
        _ALERTS_DF = pd.DataFrame({
            'drupal_location_id': ['ii']*4,
            'name': ['Library I']*4,
            'alert_id': ['9']*4,
            'closed_for': ['Lib I is closed']*4,
            'extended_closing': [False]*4,
            'alert_start': ['2023-01-01 09:00:00-05']*4,
            'alert_end': ['2023-01-01 17:00:00-05']*4,
            'polling_datetime': get_polling_times(9, 13),
            'regular_open': [time(9)]*4,
            'regular_close': [time(17)]*4})
        _FULL_DF = convert_df_types(
            pd.concat([_BASE_ALERTS_DF, _ALERTS_DF], ignore_index=True))

        _CLOSURES_DF = pd.DataFrame({
            'drupal_location_id': ['ii'],
            'name': ['Library I'],
            'alert_id': ['9'],
            'closed_for': ['Lib I is closed'],
            'is_extended_closure': [False],
            'is_full_day': [False],
            'closure_date': ['2023-01-01'],
            'closure_start': ['09:01:23'],
            'closure_end': ['12:01:23']
        })

        assert_frame_equal(
            lambda_function.get_closures(_FULL_DF), _CLOSURES_DF,
            check_like=True)
