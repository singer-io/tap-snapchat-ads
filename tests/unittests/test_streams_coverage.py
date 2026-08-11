"""
Coverage tests for tap_snapchat_ads/streams.py missing lines.
Covers: decamelize helpers, SnapchatAds methods, and sync_endpoint control-flow paths.
"""
import unittest
from unittest import mock
import pytz
from dateutil import tz as dateutil_tz
from datetime import datetime

from singer.schema import Schema
from singer import metadata as singer_metadata
from singer import utils as singer_utils

from tap_snapchat_ads.streams import (
    SnapchatAds,
    _to_snake_case,
    _decamelize_fallback,
    decamelize_record,
)


# ---------------------------------------------------------------------------
# Shared test helpers
# ---------------------------------------------------------------------------

def _make_schema(extra_props=None):
    props = {'id': {'type': 'string'}, 'name': {'type': 'string'},
             'updated_at': {'type': 'string', 'format': 'date-time'}}
    if extra_props:
        props.update(extra_props)
    return Schema.from_dict({'type': 'object', 'properties': props})


class _MockStream:
    def __init__(self, schema_dict=None):
        if schema_dict:
            self.schema = Schema.from_dict(schema_dict)
        else:
            self.schema = _make_schema()
        self.key_properties = ['id']
        self.metadata = singer_metadata.to_list(singer_metadata.new())


class _MockCatalog:
    def __init__(self, schema_dict=None):
        self._schema_dict = schema_dict
    def get_stream(self, stream_name):
        return _MockStream(self._schema_dict)


def _make_mock_client(return_value=None, side_effect=None):
    client = mock.MagicMock()
    client.base_url = 'https://test.com/v1'
    if side_effect:
        client.get.side_effect = side_effect
    else:
        client.get.return_value = return_value if return_value is not None else {}
    return client


# Base class for sync_endpoint tests - mocks out expensive side effects
class _SyncEndpointBase(unittest.TestCase):
    def _run_sync(self, stream_class, config, client, stream_name, state=None, parent_id=None):
        obj = SnapchatAds()
        obj.sync_endpoint(
            client=client,
            config=config,
            catalog=_MockCatalog(),
            state=state or {},
            stream_name=stream_name,
            stream_class=stream_class,
            sync_streams=[stream_name],
            selected_streams=[stream_name],
            parent_id=parent_id,
        )


# ---------------------------------------------------------------------------
# 1. Decamelize helper functions  (lines 38, 53, 55, 60)
# ---------------------------------------------------------------------------

class TestToSnakeCase(unittest.TestCase):
    def test_non_string_returns_unchanged(self):
        # line 38: return value when not str
        self.assertEqual(_to_snake_case(42), 42)
        self.assertIsNone(_to_snake_case(None))

    def test_camel_to_snake(self):
        self.assertEqual(_to_snake_case('camelCase'), 'camel_case')
        self.assertEqual(_to_snake_case('PascalCase'), 'pascal_case')

    def test_hyphen_to_underscore(self):
        self.assertEqual(_to_snake_case('kebab-case'), 'kebab_case')


class TestDecamelizeFallback(unittest.TestCase):
    def test_list_input(self):
        # line 53: list comprehension
        result = _decamelize_fallback([{'camelCase': 'v'}])
        self.assertEqual(result, [{'camel_case': 'v'}])

    def test_scalar_input(self):
        # line 55: return value for non-dict, non-list
        self.assertEqual(_decamelize_fallback(42), 42)
        self.assertEqual(_decamelize_fallback('hello'), 'hello')


class TestDecamelizeRecord(unittest.TestCase):
    def test_with_humps_available(self):
        # line 60: return humps.decamelize(record)
        import tap_snapchat_ads.streams as streams_module
        mock_humps = mock.MagicMock()
        mock_humps.decamelize.return_value = {'camel_case': 'v'}
        with mock.patch.object(streams_module, 'humps', mock_humps):
            result = decamelize_record({'camelCase': 'v'})
        mock_humps.decamelize.assert_called_once_with({'camelCase': 'v'})
        self.assertEqual(result, {'camel_case': 'v'})

    def test_with_humps_none(self):
        import tap_snapchat_ads.streams as streams_module
        with mock.patch.object(streams_module, 'humps', None):
            result = decamelize_record({'camelCase': 'v'})
        self.assertEqual(result, {'camel_case': 'v'})


# ---------------------------------------------------------------------------
# 2. write_schema error path  (lines 125–127)
# ---------------------------------------------------------------------------

class TestWriteSchemaOSError(unittest.TestCase):
    @mock.patch('tap_snapchat_ads.streams.singer.write_schema', side_effect=OSError('disk error'))
    def test_os_error_is_re_raised(self, _):
        obj = SnapchatAds()
        with self.assertRaises(OSError):
            obj.write_schema(_MockCatalog(), 'organizations', ['organizations'], ['organizations'])


# ---------------------------------------------------------------------------
# 3. write_record error paths  (lines 134–143)
# ---------------------------------------------------------------------------

class TestWriteRecord(unittest.TestCase):
    @mock.patch('tap_snapchat_ads.streams.singer.messages.write_record',
                side_effect=OSError('disk full'))
    def test_os_error_re_raised(self, _):
        obj = SnapchatAds()
        with self.assertRaises(OSError):
            obj.write_record('s', {'id': '1'}, singer_utils.now())

    @mock.patch('tap_snapchat_ads.streams.singer.messages.write_record',
                side_effect=TypeError('bad type'))
    def test_type_error_re_raised(self, _):
        obj = SnapchatAds()
        with self.assertRaises(TypeError):
            obj.write_record('s', {'id': '1'}, singer_utils.now())


# ---------------------------------------------------------------------------
# 4. get_bookmark paths  (lines 153–161)
# ---------------------------------------------------------------------------

class TestGetBookmark(unittest.TestCase):
    def setUp(self):
        self.obj = SnapchatAds()

    def test_no_bookmark_field_returns_default(self):
        # line 153-154
        state = {'bookmarks': {'organizations': {'key': 'val'}}}
        result = self.obj.get_bookmark(state, 'organizations', 'default', bookmark_field=None)
        self.assertEqual(result, 'default')

    def test_with_parent_and_parent_id(self):
        # lines 156-157
        state = {'bookmarks': {'ad_accounts': {'updated_at(parent_org_id:123)': '2022-01-01T00:00:00Z'}}}
        result = self.obj.get_bookmark(state, 'ad_accounts', 'default', 'updated_at', 'org', '123')
        self.assertEqual(result, '2022-01-01T00:00:00Z')

    def test_without_parent(self):
        # lines 158-161
        state = {'bookmarks': {'organizations': {'updated_at': '2022-01-01T00:00:00Z'}}}
        result = self.obj.get_bookmark(state, 'organizations', 'default', 'updated_at')
        self.assertEqual(result, '2022-01-01T00:00:00Z')


# ---------------------------------------------------------------------------
# 5. transform_datetime  (lines 193–195)
# ---------------------------------------------------------------------------

class TestTransformDatetime(unittest.TestCase):
    def test_converts_iso_string(self):
        obj = SnapchatAds()
        result = obj.transform_datetime('2021-01-15T10:30:45Z')
        self.assertIsNotNone(result)
        self.assertIn('2021', result)


# ---------------------------------------------------------------------------
# 6. process_records  (lines 208–249)
# ---------------------------------------------------------------------------

class TestProcessRecords(unittest.TestCase):
    @mock.patch('tap_snapchat_ads.streams.singer.messages.write_record')
    def test_no_bookmark_field(self, mock_write):
        obj = SnapchatAds()
        _, count = obj.process_records(
            catalog=_MockCatalog(), stream_name='t',
            records=[{'id': '1', 'name': 'x'}],
            time_extracted=singer_utils.now(),
            bookmark_field=None, max_bookmark_value=None,
            last_datetime='2021-01-01T00:00:00Z',
        )
        self.assertEqual(count, 1)

    @mock.patch('tap_snapchat_ads.streams.singer.messages.write_record')
    def test_new_record_written(self, mock_write):
        obj = SnapchatAds()
        max_bm, count = obj.process_records(
            catalog=_MockCatalog(), stream_name='t',
            records=[{'id': '1', 'updated_at': '2022-04-16T05:44:39Z'}],
            time_extracted=singer_utils.now(),
            bookmark_field='updated_at',
            max_bookmark_value=None,          # triggers "if not max_bookmark_value"
            last_datetime='2021-01-01T00:00:00Z',
        )
        self.assertEqual(count, 1)

    @mock.patch('tap_snapchat_ads.streams.singer.messages.write_record')
    def test_old_record_not_written(self, mock_write):
        obj = SnapchatAds()
        _, count = obj.process_records(
            catalog=_MockCatalog(), stream_name='t',
            records=[{'id': '1', 'updated_at': '2020-01-01T00:00:00Z'}],
            time_extracted=singer_utils.now(),
            bookmark_field='updated_at',
            max_bookmark_value='2021-01-01T00:00:00Z',
            last_datetime='2021-01-01T00:00:00Z',
        )
        self.assertEqual(count, 0)
        mock_write.assert_not_called()

    @mock.patch('tap_snapchat_ads.streams.singer.messages.write_record')
    def test_null_bookmark_skips_record(self, mock_write):
        # lines 225-227: null bookmark_date → LOGGER.warning + continue
        # Schema must allow null so Transformer keeps the None value in the record
        nullable_schema = {
            'type': 'object',
            'properties': {
                'id': {'type': ['null', 'string']},
                'updated_at': {'type': ['null', 'string'], 'format': 'date-time'},
            }
        }
        obj = SnapchatAds()
        _, count = obj.process_records(
            catalog=_MockCatalog(schema_dict=nullable_schema), stream_name='t',
            records=[{'id': '1', 'updated_at': None}],
            time_extracted=singer_utils.now(),
            bookmark_field='updated_at',
            max_bookmark_value='2021-01-01T00:00:00Z',
            last_datetime='2021-01-01T00:00:00Z',
        )
        self.assertEqual(count, 0)
        mock_write.assert_not_called()

    @mock.patch('tap_snapchat_ads.streams.singer.messages.write_record')
    def test_higher_bookmark_updates_max(self, mock_write):
        obj = SnapchatAds()
        max_bm, _ = obj.process_records(
            catalog=_MockCatalog(), stream_name='t',
            records=[{'id': '1', 'updated_at': '2022-04-16T05:44:39Z'}],
            time_extracted=singer_utils.now(),
            bookmark_field='updated_at',
            max_bookmark_value='2021-01-01T00:00:00Z',   # lower → should be updated
            last_datetime='2021-01-01T00:00:00Z',
        )
        self.assertGreater(max_bm, '2021-01-01T00:00:00Z')


# ---------------------------------------------------------------------------
# 7. remove_minutes_local / remove_hours_local  (lines 256–258, 265–267)
# ---------------------------------------------------------------------------

class TestRemoveTimeComponents(unittest.TestCase):
    def test_remove_minutes_local(self):
        obj = SnapchatAds()
        dttm = datetime(2021, 1, 15, 10, 30, 45, tzinfo=pytz.UTC)
        result = obj.remove_minutes_local(dttm, dateutil_tz.gettz('UTC'))
        self.assertEqual(result, '2021-01-15T10:00:00Z')

    def test_remove_hours_local(self):
        obj = SnapchatAds()
        dttm = datetime(2021, 1, 15, 10, 30, 45, tzinfo=pytz.UTC)
        result = obj.remove_hours_local(dttm, dateutil_tz.gettz('UTC'))
        self.assertEqual(result, '2021-01-15T00:00:00Z')


# ---------------------------------------------------------------------------
# 8. extract_selected_profile_data edge cases  (lines 286, 293, 296–303)
# ---------------------------------------------------------------------------

class TestExtractSelectedProfileData(unittest.TestCase):
    def test_adaccounts_no_match_logs_warn(self):
        # line 286: LOGGER.warn for adaccounts with no matching org
        config = {'org_account_ids': [{'organisation_id': 'other', 'ad_accounts': ['a1']}]}
        result = SnapchatAds.extract_selected_profile_data(
            config, mock.MagicMock(), 'adaccounts', parent_id='non_existent')
        self.assertEqual(result, {'adaccounts': []})

    def test_api_call_success(self):
        # lines 295–303
        mock_client = mock.MagicMock()
        mock_client.get.return_value = {'organizations': [{'id': 'org1'}]}
        config = {'org_account_ids': [{'organisation_id': 'org1', 'ad_accounts': []}]}
        result = SnapchatAds.extract_selected_profile_data(config, mock_client, 'organizations')
        self.assertEqual(result['organizations'], [{'id': 'org1'}])

    def test_api_exception_propagated(self):
        # lines 296–302: exception logged and re-raised
        mock_client = mock.MagicMock()
        mock_client.get.side_effect = RuntimeError('boom')
        config = {'org_account_ids': [{'organisation_id': 'org1', 'ad_accounts': []}]}
        with self.assertRaises(RuntimeError):
            SnapchatAds.extract_selected_profile_data(config, mock_client, 'organizations')


# ---------------------------------------------------------------------------
# 9. sync_endpoint – view_attribution_window HOUR  (line 352)
# ---------------------------------------------------------------------------

class TestSyncEndpointViewAttrHour(_SyncEndpointBase):
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_bookmark')
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_schema')
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.process_records',
                return_value=('2022-01-01T00:00:00Z', 0))
    def test_hourly_view_attribution_window(self, mock_proc, mock_schema, mock_bm):
        class _S(SnapchatAds):
            tap_stream_id = 'orgs'
            key_properties = ['id']
            replication_method = 'INCREMENTAL'
            replication_keys = ['updated_at']
            path = 'organizations'
            data_key_array = 'organizations'
            data_key_record = 'organization'
            paging = False
            params = {}
            parent_stream = parent = grandparent_stream = great_grandparent_stream = None
            json_schema = bookmark_query_field_from = bookmark_query_field_to = None
            date_window_size = 1
            targeting_country_ind = False
            targeting_group = targeting_type = None
            children = []

        self._run_sync(_S, {'start_date': '2022-01-01T00:00:00Z',
                            'view_attribution_window': '1_HOUR'},
                       _make_mock_client({}), 'organizations')


# ---------------------------------------------------------------------------
# 10. sync_endpoint – targeting_country_ind  (line 365)
# ---------------------------------------------------------------------------

class TestSyncEndpointTargetingCountry(_SyncEndpointBase):
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_bookmark')
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_schema')
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.process_records',
                return_value=('2022-01-01T00:00:00Z', 0))
    def test_country_code_list_from_config(self, mock_proc, mock_schema, mock_bm):
        class _S(SnapchatAds):
            tap_stream_id = 'targeting_regions_test'
            key_properties = ['id']
            replication_method = 'FULL_TABLE'
            replication_keys = []
            path = 'targeting/geo/{country_code}/region'
            data_key_array = 'targeting_dimensions'
            data_key_record = '{targeting_type}'
            paging = False
            params = {}
            parent_stream = parent = grandparent_stream = great_grandparent_stream = None
            json_schema = bookmark_query_field_from = bookmark_query_field_to = None
            date_window_size = 1
            targeting_country_ind = True   # line 365
            targeting_group = 'geo'
            targeting_type = 'region'
            children = []

        self._run_sync(_S, {'start_date': '2022-01-01T00:00:00Z',
                            'targeting_country_codes': 'us,ca'},
                       _make_mock_client({}), 'targeting_regions_test')


# ---------------------------------------------------------------------------
# 11. sync_endpoint – timezone_desc  (line 373)
# ---------------------------------------------------------------------------

class TestSyncEndpointTimezoneDesc(_SyncEndpointBase):
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_bookmark')
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_schema')
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.process_records',
                return_value=('2022-01-01T00:00:00Z', 0))
    def test_explicit_timezone(self, mock_proc, mock_schema, mock_bm):
        class _S(SnapchatAds):
            tap_stream_id = 'orgs2'
            key_properties = ['id']
            replication_method = 'INCREMENTAL'
            replication_keys = ['updated_at']
            path = 'organizations'
            data_key_array = 'organizations'
            data_key_record = 'organization'
            paging = False
            params = {}
            parent_stream = parent = grandparent_stream = great_grandparent_stream = None
            json_schema = bookmark_query_field_from = bookmark_query_field_to = None
            date_window_size = 1
            targeting_country_ind = False
            targeting_group = targeting_type = None
            children = []

        obj = SnapchatAds()
        obj.sync_endpoint(
            client=_make_mock_client({}),
            config={'start_date': '2022-01-01T00:00:00Z'},
            catalog=_MockCatalog(),
            state={},
            stream_name='organizations',
            stream_class=_S,
            sync_streams=['organizations'],
            selected_streams=['organizations'],
            timezone_desc='America/New_York',   # line 373
        )


# ---------------------------------------------------------------------------
# 12. sync_endpoint – stats stream: DAY granularity  (lines 360, 385, 390–394, 414–419)
# ---------------------------------------------------------------------------

class TestSyncEndpointStatsDAY(_SyncEndpointBase):
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_bookmark')
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_schema')
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.process_records',
                return_value=('2022-01-01T00:00:00Z', 0))
    def test_day_granularity_stats(self, mock_proc, mock_schema, mock_bm):
        class _S(SnapchatAds):
            tap_stream_id = 'ad_account_stats_daily_cov'
            key_properties = ['id', 'start_time']
            replication_method = 'INCREMENTAL'
            replication_keys = ['end_time']
            bookmark_query_field_from = 'start_time'
            bookmark_query_field_to = 'end_time'
            path = 'adaccounts/{parent_id}/stats'
            data_key_array = 'timeseries_stats'
            data_key_record = 'timeseries_stat'
            date_window_size = 30
            paging = False
            parent = 'ad_account'
            params = {'granularity': 'DAY'}
            parent_stream = grandparent_stream = great_grandparent_stream = None
            json_schema = None
            targeting_country_ind = False
            targeting_group = targeting_type = None
            children = []

        self._run_sync(_S, {'start_date': '2021-01-01T00:00:00Z'},
                       _make_mock_client({}), 'ad_account_stats_daily_cov',
                       parent_id='acc1')

    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_bookmark')
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_schema')
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.process_records',
                return_value=('2022-01-01T00:00:00Z', 0))
    @mock.patch.object(SnapchatAds, 'remove_hours_local',
                       return_value='2021-01-15T00:00:00Z')
    def test_day_granularity_same_start_end_adjustment(self, mock_remove, mock_proc,
                                                       mock_schema, mock_bm):
        # lines 417-419: window adjustment when remove_hours gives same result for start+end
        # return_value (not side_effect) so the mock handles any number of loop iterations

        class _S(SnapchatAds):
            tap_stream_id = 'ad_account_stats_daily_adj'
            key_properties = ['id', 'start_time']
            replication_method = 'INCREMENTAL'
            replication_keys = ['end_time']
            bookmark_query_field_from = 'start_time'
            bookmark_query_field_to = 'end_time'
            path = 'adaccounts/{parent_id}/stats'
            data_key_array = 'timeseries_stats'
            data_key_record = 'timeseries_stat'
            date_window_size = 30
            paging = False
            parent = 'ad_account'
            params = {'granularity': 'DAY'}
            parent_stream = grandparent_stream = great_grandparent_stream = None
            json_schema = None
            targeting_country_ind = False
            targeting_group = targeting_type = None
            children = []

        # Use recent start_date + short attribution so only ONE date window runs
        self._run_sync(_S, {'start_date': '2026-08-01T00:00:00Z',
                            'swipe_up_attribution_window': '1_DAY',
                            'view_attribution_window': '1_DAY'},
                       _make_mock_client({}), 'ad_account_stats_daily_adj',
                       parent_id='acc1')


# ---------------------------------------------------------------------------
# 13. sync_endpoint – stats stream: HOUR granularity  (lines 420–428)
# ---------------------------------------------------------------------------

class TestSyncEndpointStatsHOUR(_SyncEndpointBase):
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_bookmark')
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_schema')
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.process_records',
                return_value=('2022-01-01T00:00:00Z', 0))
    def test_hour_granularity_stats(self, mock_proc, mock_schema, mock_bm):
        class _S(SnapchatAds):
            tap_stream_id = 'ad_account_stats_hourly_cov'
            key_properties = ['id', 'start_time']
            replication_method = 'INCREMENTAL'
            replication_keys = ['end_time']
            bookmark_query_field_from = 'start_time'
            bookmark_query_field_to = 'end_time'
            path = 'adaccounts/{parent_id}/stats'
            data_key_array = 'timeseries_stats'
            data_key_record = 'timeseries_stat'
            date_window_size = 7
            paging = False
            parent = 'ad_account'
            params = {'granularity': 'HOUR'}
            parent_stream = grandparent_stream = great_grandparent_stream = None
            json_schema = None
            targeting_country_ind = False
            targeting_group = targeting_type = None
            children = []

        self._run_sync(_S, {'start_date': '2021-01-01T00:00:00Z'},
                       _make_mock_client({}), 'ad_account_stats_hourly_cov',
                       parent_id='acc1')

    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_bookmark')
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_schema')
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.process_records',
                return_value=('2022-01-01T00:00:00Z', 0))
    @mock.patch.object(SnapchatAds, 'remove_minutes_local',
                       return_value='2021-01-15T10:00:00Z')
    def test_hour_granularity_same_start_end_adjustment(self, mock_remove, mock_proc,
                                                        mock_schema, mock_bm):
        # lines 423-428: same-hour window adjustment
        # return_value (not side_effect) so the mock handles any number of loop iterations

        class _S(SnapchatAds):
            tap_stream_id = 'ad_account_stats_hourly_adj'
            key_properties = ['id', 'start_time']
            replication_method = 'INCREMENTAL'
            replication_keys = ['end_time']
            bookmark_query_field_from = 'start_time'
            bookmark_query_field_to = 'end_time'
            path = 'adaccounts/{parent_id}/stats'
            data_key_array = 'timeseries_stats'
            data_key_record = 'timeseries_stat'
            date_window_size = 7
            paging = False
            parent = 'ad_account'
            params = {'granularity': 'HOUR'}
            parent_stream = grandparent_stream = great_grandparent_stream = None
            json_schema = None
            targeting_country_ind = False
            targeting_group = targeting_type = None
            children = []

        # Use recent start_date + short attribution window so only ONE date window iteration
        # runs (3 mock calls to remove_minutes_local: start, end-same, end+1h)
        self._run_sync(_S, {'start_date': '2026-08-01T00:00:00Z',
                            'swipe_up_attribution_window': '1_DAY',
                            'view_attribution_window': '1_DAY'},
                       _make_mock_client({}), 'ad_account_stats_hourly_adj',
                       parent_id='acc1')


# ---------------------------------------------------------------------------
# 14. sync_endpoint – end_window advancement  (line 689)
# ---------------------------------------------------------------------------

class TestSyncEndpointEndWindowAdvance(_SyncEndpointBase):
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_bookmark')
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_schema')
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.process_records',
                return_value=('2022-01-01T00:00:00Z', 0))
    def test_multiple_date_windows_advances_end(self, mock_proc, mock_schema, mock_bm):
        # line 689: end_window = next_end_window (multiple iterations needed)
        # Use 7-day window with attribution=28 → several iterations before reaching now
        class _S(SnapchatAds):
            tap_stream_id = 'ad_stats_hourly_win'
            key_properties = ['id', 'start_time']
            replication_method = 'INCREMENTAL'
            replication_keys = ['end_time']
            bookmark_query_field_from = 'start_time'
            bookmark_query_field_to = 'end_time'
            path = 'adaccounts/{parent_id}/stats'
            data_key_array = 'timeseries_stats'
            data_key_record = 'timeseries_stat'
            date_window_size = 7
            paging = False
            parent = 'ad_account'
            params = {'granularity': 'HOUR'}
            parent_stream = grandparent_stream = great_grandparent_stream = None
            json_schema = None
            targeting_country_ind = False
            targeting_group = targeting_type = None
            children = []

        self._run_sync(_S,
                       {'start_date': '2021-01-01T00:00:00Z',
                        'swipe_up_attribution_window': '1_DAY',
                        'view_attribution_window': '1_DAY'},
                       _make_mock_client({}), 'ad_stats_hourly_win',
                       parent_id='acc1')


# ---------------------------------------------------------------------------
# 15. sync_endpoint – org_account_ids extract path  (lines 481–482)
# ---------------------------------------------------------------------------

class TestSyncEndpointOrgAccountIds(_SyncEndpointBase):
    @mock.patch.object(SnapchatAds, 'extract_selected_profile_data',
                       return_value={'organizations': [
                           {'sub_request_status': 'SUCCESS',
                            'organization': {'id': 'org1', 'updated_at': '2022-01-01T00:00:00Z'}}
                       ]})
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_bookmark')
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_schema')
    @mock.patch('tap_snapchat_ads.streams.singer.messages.write_record')
    def test_extract_called_for_organizations(self, mock_write, mock_schema, mock_bm,
                                              mock_extract):
        class _S(SnapchatAds):
            tap_stream_id = 'organizations'
            key_properties = ['id']
            replication_method = 'INCREMENTAL'
            replication_keys = ['updated_at']
            path = 'organizations'
            data_key_array = 'organizations'
            data_key_record = 'organization'
            paging = False
            params = {}
            parent_stream = parent = grandparent_stream = great_grandparent_stream = None
            json_schema = bookmark_query_field_from = bookmark_query_field_to = None
            date_window_size = 1
            targeting_country_ind = False
            targeting_group = targeting_type = None
            children = []

        self._run_sync(
            _S,
            {'start_date': '2022-01-01T00:00:00Z',
             'org_account_ids': [{'organisation_id': 'org1', 'ad_accounts': []}]},
            _make_mock_client({}),
            'organizations',
        )
        mock_extract.assert_called()


# ---------------------------------------------------------------------------
# 16. sync_endpoint – exception path  (lines 485–488)
# ---------------------------------------------------------------------------

class TestSyncEndpointException(_SyncEndpointBase):
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_schema')
    def test_client_exception_propagated(self, mock_schema):
        class _S(SnapchatAds):
            tap_stream_id = 'orgs_exc'
            key_properties = ['id']
            replication_method = 'INCREMENTAL'
            replication_keys = ['updated_at']
            path = 'organizations'
            data_key_array = 'organizations'
            data_key_record = 'organization'
            paging = False
            params = {}
            parent_stream = parent = grandparent_stream = great_grandparent_stream = None
            json_schema = bookmark_query_field_from = bookmark_query_field_to = None
            date_window_size = 1
            targeting_country_ind = False
            targeting_group = targeting_type = None
            children = []

        with self.assertRaises(RuntimeError):
            self._run_sync(_S, {'start_date': '2022-01-01T00:00:00Z'},
                           _make_mock_client(side_effect=RuntimeError('api down')),
                           'organizations')


# ---------------------------------------------------------------------------
# 17. sync_endpoint – request_status != SUCCESS  (line 499)
# ---------------------------------------------------------------------------

class TestSyncEndpointBadRequestStatus(_SyncEndpointBase):
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_schema')
    def test_raises_on_bad_request_status(self, mock_schema):
        class _S(SnapchatAds):
            tap_stream_id = 'orgs_bad'
            key_properties = ['id']
            replication_method = 'INCREMENTAL'
            replication_keys = ['updated_at']
            path = 'organizations'
            data_key_array = 'organizations'
            data_key_record = 'organization'
            paging = False
            params = {}
            parent_stream = parent = grandparent_stream = great_grandparent_stream = None
            json_schema = bookmark_query_field_from = bookmark_query_field_to = None
            date_window_size = 1
            targeting_country_ind = False
            targeting_group = targeting_type = None
            children = []

        with self.assertRaises(RuntimeError):
            self._run_sync(_S, {'start_date': '2022-01-01T00:00:00Z'},
                           _make_mock_client({'request_status': 'ERROR'}),
                           'organizations')


# ---------------------------------------------------------------------------
# 18. sync_endpoint – stats de-nesting  (lines 510–539)
# ---------------------------------------------------------------------------

class TestSyncEndpointStatsDenesting(_SyncEndpointBase):
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_bookmark')
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_schema')
    @mock.patch('tap_snapchat_ads.streams.singer.messages.write_record')
    def test_stats_timeseries_denested(self, mock_write, mock_schema, mock_bm):
        class _S(SnapchatAds):
            tap_stream_id = 'ad_account_stats_daily'
            key_properties = ['id', 'start_time']
            replication_method = 'INCREMENTAL'
            replication_keys = ['end_time']
            bookmark_query_field_from = 'start_time'
            bookmark_query_field_to = 'end_time'
            path = 'adaccounts/{parent_id}/stats'
            data_key_array = 'timeseries_stats'
            data_key_record = 'timeseries_stat'
            date_window_size = 30
            paging = False
            parent = 'ad_account'
            params = {'granularity': 'DAY'}
            parent_stream = grandparent_stream = great_grandparent_stream = None
            json_schema = None
            targeting_country_ind = False
            targeting_group = targeting_type = None
            children = []

        stats_data = {
            'request_status': 'SUCCESS',
            'timeseries_stats': [{
                'timeseries_stat': {
                    'id': 'acc1',
                    'start_time': '2021-01-15T00:00:00.000Z',
                    'end_time':   '2021-01-16T00:00:00.000Z',
                    'timeseries': [{
                        'start_time': '2021-01-15T00:00:00.000Z',
                        'end_time':   '2021-01-16T00:00:00.000Z',
                        'stats': {'spend': 100},
                    }],
                }
            }],
        }
        self._run_sync(_S, {'start_date': '2021-01-01T00:00:00Z'},
                       _make_mock_client(stats_data),
                       'ad_account_stats_daily', parent_id='acc1')

    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_schema')
    def test_stats_missing_id_or_start_time_raises(self, mock_schema):
        # lines 533–537: missing 'id' or 'start_time' → RuntimeError
        class _S(SnapchatAds):
            tap_stream_id = 'campaign_stats_daily'
            key_properties = ['id', 'start_time']
            replication_method = 'INCREMENTAL'
            replication_keys = ['end_time']
            bookmark_query_field_from = 'start_time'
            bookmark_query_field_to = 'end_time'
            path = 'campaigns/{parent_id}/stats'
            data_key_array = 'timeseries_stats'
            data_key_record = 'timeseries_stat'
            date_window_size = 30
            paging = False
            parent = 'campaign'
            params = {'granularity': 'DAY'}
            parent_stream = grandparent_stream = great_grandparent_stream = None
            json_schema = None
            targeting_country_ind = False
            targeting_group = targeting_type = None
            children = []

        bad_stats_data = {
            'request_status': 'SUCCESS',
            'timeseries_stats': [{
                'timeseries_stat': {
                    'timeseries': [{'stats': {'spend': 0}}],  # missing 'id' and 'start_time'
                }
            }],
        }
        with self.assertRaises(RuntimeError):
            self._run_sync(_S, {'start_date': '2021-01-01T00:00:00Z'},
                           _make_mock_client(bad_stats_data),
                           'campaign_stats_daily', parent_id='camp1')


# ---------------------------------------------------------------------------
# 19. sync_endpoint – sub_request_status != SUCCESS  (line 549)
# ---------------------------------------------------------------------------

class TestSyncEndpointBadSubRequestStatus(_SyncEndpointBase):
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_schema')
    def test_bad_sub_request_raises(self, mock_schema):
        class _S(SnapchatAds):
            tap_stream_id = 'orgs_sub'
            key_properties = ['id']
            replication_method = 'INCREMENTAL'
            replication_keys = ['updated_at']
            path = 'organizations'
            data_key_array = 'organizations'
            data_key_record = 'organization'
            paging = False
            params = {}
            parent_stream = parent = grandparent_stream = great_grandparent_stream = None
            json_schema = bookmark_query_field_from = bookmark_query_field_to = None
            date_window_size = 1
            targeting_country_ind = False
            targeting_group = targeting_type = None
            children = []

        with self.assertRaises(RuntimeError):
            self._run_sync(
                _S, {'start_date': '2022-01-01T00:00:00Z'},
                _make_mock_client({'request_status': 'SUCCESS',
                                   'organizations': [{'sub_request_status': 'ERROR'}]}),
                'organizations',
            )


# ---------------------------------------------------------------------------
# 20. sync_endpoint – targeting transforms  (lines 554–567)
# ---------------------------------------------------------------------------

class TestSyncEndpointTargetingTransforms(_SyncEndpointBase):
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_bookmark')
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_schema')
    @mock.patch('tap_snapchat_ads.streams.singer.messages.write_record')
    def test_geo_targeting_with_country_code(self, mock_write, mock_schema, mock_bm):
        # lines 554-562: targeting_group == 'geo', country_code != 'none'
        class _S(SnapchatAds):
            tap_stream_id = 'targeting_regions_test'
            key_properties = ['id']
            replication_method = 'FULL_TABLE'
            replication_keys = []
            path = 'targeting/geo/{country_code}/region'
            data_key_array = 'targeting_dimensions'
            data_key_record = '{targeting_type}'
            paging = False
            params = {}
            parent_stream = parent = grandparent_stream = great_grandparent_stream = None
            json_schema = bookmark_query_field_from = bookmark_query_field_to = None
            date_window_size = 1
            targeting_country_ind = True
            targeting_group = 'geo'
            targeting_type = 'region'
            children = []

        # The geo code does record.get(targeting_type, {}).get('id'), so the record
        # extracted as data_key_record must have a nested targeting_type key
        geo_data = {
            'request_status': 'SUCCESS',
            'targeting_dimensions': [{
                'sub_request_status': 'SUCCESS',
                'region': {
                    'region': {'id': 'US-CA', 'name': 'California'},
                },
            }],
        }
        self._run_sync(_S,
                       {'start_date': '2022-01-01T00:00:00Z',
                        'targeting_country_codes': 'us'},
                       _make_mock_client(geo_data),
                       'targeting_regions_test')

    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_bookmark')
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_schema')
    @mock.patch('tap_snapchat_ads.streams.singer.messages.write_record')
    def test_postal_code_targeting(self, mock_write, mock_schema, mock_bm):
        # lines 563-567: targeting_type == 'postal_code'
        class _S(SnapchatAds):
            tap_stream_id = 'targeting_postal_codes_test'
            key_properties = ['id']
            replication_method = 'FULL_TABLE'
            replication_keys = []
            path = 'targeting/geo/{country_code}/postal_code'
            data_key_array = 'targeting_dimensions'
            data_key_record = '{targeting_type}'
            paging = False
            params = {}
            parent_stream = parent = grandparent_stream = great_grandparent_stream = None
            json_schema = bookmark_query_field_from = bookmark_query_field_to = None
            date_window_size = 1
            targeting_country_ind = True
            targeting_group = 'geo'
            targeting_type = 'postal_code'
            children = []

        postal_data = {
            'request_status': 'SUCCESS',
            'targeting_dimensions': [{
                'sub_request_status': 'SUCCESS',
                'postal_code': {'postalCode': '90210'},
            }],
        }
        self._run_sync(_S,
                       {'start_date': '2022-01-01T00:00:00Z',
                        'targeting_country_codes': 'us'},
                       _make_mock_client(postal_data),
                       'targeting_postal_codes_test')


# ---------------------------------------------------------------------------
# 21. sync_endpoint – decamelize exception  (lines 577–580)
# ---------------------------------------------------------------------------

class TestSyncEndpointDecamelizeException(_SyncEndpointBase):
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_schema')
    @mock.patch('tap_snapchat_ads.streams.decamelize_record',
                side_effect=Exception('decode error'))
    def test_decamelize_exception_propagated(self, mock_decamelize, mock_schema):
        class _S(SnapchatAds):
            tap_stream_id = 'orgs_decam'
            key_properties = ['id']
            replication_method = 'INCREMENTAL'
            replication_keys = ['updated_at']
            path = 'organizations'
            data_key_array = 'organizations'
            data_key_record = 'organization'
            paging = False
            params = {}
            parent_stream = parent = grandparent_stream = great_grandparent_stream = None
            json_schema = bookmark_query_field_from = bookmark_query_field_to = None
            date_window_size = 1
            targeting_country_ind = False
            targeting_group = targeting_type = None
            children = []

        data = {
            'request_status': 'SUCCESS',
            'organizations': [{'sub_request_status': 'SUCCESS', 'organization': {'id': 'o1'}}],
        }
        with self.assertRaises(Exception):
            self._run_sync(_S, {'start_date': '2022-01-01T00:00:00Z'},
                           _make_mock_client(data), 'organizations')

    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_schema')
    @mock.patch('tap_snapchat_ads.streams.decamelize_record',
                side_effect=Exception('decode error'))
    def test_decamelize_exception_in_stats_path(self, mock_decamelize, mock_schema):
        # lines 528-530: exception handler in stats de-nesting path
        class _S(SnapchatAds):
            tap_stream_id = 'ad_account_stats_daily'
            key_properties = ['id', 'start_time']
            replication_method = 'INCREMENTAL'
            replication_keys = ['end_time']
            bookmark_query_field_from = 'start_time'
            bookmark_query_field_to = 'end_time'
            path = 'adaccounts/{parent_id}/stats'
            data_key_array = 'timeseries_stats'
            data_key_record = 'timeseries_stat'
            date_window_size = 30
            paging = False
            parent = 'ad_account'
            params = {'granularity': 'DAY'}
            parent_stream = grandparent_stream = great_grandparent_stream = None
            json_schema = None
            targeting_country_ind = False
            targeting_group = targeting_type = None
            children = []

        stats_data = {
            'request_status': 'SUCCESS',
            'timeseries_stats': [{
                'timeseries_stat': {
                    'id': 'acc1',
                    'start_time': '2021-01-15T00:00:00.000Z',
                    'end_time':   '2021-01-16T00:00:00.000Z',
                    'timeseries': [{
                        'start_time': '2021-01-15T00:00:00.000Z',
                        'end_time':   '2021-01-16T00:00:00.000Z',
                        'stats': {'spend': 100},
                    }],
                }
            }],
        }
        with self.assertRaises(Exception):
            self._run_sync(_S, {'start_date': '2021-01-01T00:00:00Z'},
                           _make_mock_client(stats_data),
                           'ad_account_stats_daily', parent_id='acc1')


# ---------------------------------------------------------------------------
# 22. sync_endpoint – missing required key in record  (lines 585–588)
# ---------------------------------------------------------------------------

class TestSyncEndpointMissingKey(_SyncEndpointBase):
    @mock.patch('tap_snapchat_ads.streams.SnapchatAds.write_schema')
    def test_missing_id_key_raises(self, mock_schema):
        class _S(SnapchatAds):
            tap_stream_id = 'orgs_miss'
            key_properties = ['id']
            replication_method = 'INCREMENTAL'
            replication_keys = ['updated_at']
            path = 'organizations'
            data_key_array = 'organizations'
            data_key_record = 'organization'
            paging = False
            params = {}
            parent_stream = parent = grandparent_stream = great_grandparent_stream = None
            json_schema = bookmark_query_field_from = bookmark_query_field_to = None
            date_window_size = 1
            targeting_country_ind = False
            targeting_group = targeting_type = None
            children = []

        data = {
            'request_status': 'SUCCESS',
            'organizations': [{'sub_request_status': 'SUCCESS',
                                'organization': {'name': 'no_id_here'}}],
        }
        with self.assertRaises(RuntimeError):
            self._run_sync(_S, {'start_date': '2022-01-01T00:00:00Z'},
                           _make_mock_client(data), 'organizations')
