# streams: API URL endpoints to be called
# properties:
#   <root node>: Plural stream name for the endpoint
#   path: API endpoint relative path, when added to the base URL, creates the full path
#   key_properties: Primary key fields for identifying an endpoint record.
#   replication_method: INCREMENTAL or FULL_TABLE
#   replication_keys: bookmark_field(s), typically a date-time, used for filtering the results
#        and setting the state
#   data_key: JSON element containing the records for the endpoint
#   api_method: GET or POST; default = 'GET'
#   params: Query, sort, and other endpoint specific parameters; default = {}
#   pagination: types are none, body, params; default = 'none'
#       none = no pagination
#       body = POST has startRow and rowLimit in body payload
#       params = GET has startRow and rowLimit in URL query params
#   sub_types: list of sub_types for endpoint looping; delfault = ['self']
#   bookmark_type: Data type for bookmark, integer or datetime

# pylint: disable=line-too-long
import pytz
import math
import humps
from datetime import timedelta
import singer
from singer import Transformer, metadata, metrics, utils
from singer.utils import strptime_to_utc, strftime

ALL_STATS_FIELDS = 'android_installs,attachment_avg_view_time_millis,attachment_impressions,attachment_quartile_1,attachment_quartile_2,attachment_quartile_3,attachment_total_view_time_millis,attachment_view_completion,avg_screen_time_millis,avg_view_time_millis,impressions,ios_installs,quartile_1,quartile_2,quartile_3,screen_time_millis,spend,swipe_up_percent,swipes,total_installs,video_views,video_views_time_based,video_views_15s,view_completion,view_time_millis,conversion_purchases,conversion_purchases_value,conversion_save,conversion_start_checkout,conversion_add_cart,conversion_view_content,conversion_add_billing,conversion_sign_ups,conversion_searches,conversion_level_completes,conversion_app_opens,conversion_page_views,conversion_subscribe,conversion_ad_click,conversion_ad_view,conversion_complete_tutorial,conversion_invite,conversion_login,conversion_share,conversion_reserve,conversion_achievement_unlocked,conversion_add_to_wishlist,conversion_spend_credits,conversion_rate,conversion_start_trial,conversion_list_view,custom_event_1,custom_event_2,custom_event_3,custom_event_4,custom_event_5,attachment_frequency,attachment_uniques,frequency,uniques'

LOGGER = singer.get_logger()
BASE_URL = 'https://adsapi.snapchat.com/v1'

# Currently syncing sets the stream currently being delivered in the state.
# If the integration is interrupted, this state property is used to identify
#  the starting point to continue from.
# Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
def update_currently_syncing(state, stream_name):
    if (stream_name is None) and ('currently_syncing' in state):
        del state['currently_syncing']
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)

class SnapchatAds:
    tap_stream_id = None
    key_properties = []
    replication_method = None
    replication_keys = []
    path = None
    data_key_array = None
    data_key_record = ""
    paging = False
    params = {}
    parent_stream = None
    parent = None
    grandparent_stream = None
    great_grandparent_stream = None
    json_schema = None
    bookmark_query_field_from = None
    bookmark_query_field_to = None
    date_window_size = 1
    targeting_country_ind = False
    targeting_group = None
    targeting_type = None
    children = []

    # To write schema in output
    def write_schema(self, catalog, stream_name, sync_streams , selected_streams):
        """
        To write schema in output
        """
        stream = catalog.get_stream(stream_name)
        schema = stream.schema.to_dict()
        try:
            # Write_schema for the stream if it is selected in catalog
            if stream_name in selected_streams and stream_name in sync_streams:
                singer.write_schema(stream_name, schema, stream.key_properties)
        except OSError as err:
            LOGGER.error('OS Error writing schema for: {}'.format(stream_name))
            raise err

    # To write record in output
    def write_record(self, stream_name, record, time_extracted):
        """
        To write records in sync mode
        """
        try:
            singer.messages.write_record(stream_name, record, time_extracted=time_extracted)
        except OSError as err:
            LOGGER.error('OS Error writing record for: {}'.format(stream_name))
            LOGGER.error('Stream: {}, record: {}'.format(stream_name, record))
            raise err
        except TypeError as err:
            LOGGER.error('Type Error writing record for: {}'.format(stream_name))
            LOGGER.error('Stream: {}, record: {}'.format(stream_name, record))
            raise err

    # To get bookmark from state.json or config.json
    def get_bookmark(self, state, stream, default, bookmark_field=None, parent=None, parent_id=None):
        """
        Read bookmark from state.json or config.json
        """
        if (state is None) or ('bookmarks' not in state):
            return default

        if bookmark_field is None:
            return default

        if parent and parent_id:
            key = '{}(parent_{}_id:{})'.format(bookmark_field, parent, parent_id)
        else:
            key = bookmark_field

        return (
            state
            .get('bookmarks', {})
            .get(stream, {})
            .get(key, default)
        )

    # To write new bookmark in output
    def write_bookmark(self, state, stream, value, bookmark_field=None, parent=None, parent_id=None):
        """
        To write bookmark in sync mode
        """
        if parent and parent_id:
            key = '{}(parent_{}_id:{})'.format(bookmark_field, parent, parent_id)
        else:
            key = bookmark_field
        if 'bookmarks' not in state:
            state['bookmarks'] = {}
        if stream not in state['bookmarks']:
            state['bookmarks'][stream] = {}

        state['bookmarks'][stream][key] = value
        LOGGER.info('Write state for Stream: {}, {} ID: {}, value: {}'.format(
            stream, parent, parent_id, value))
        singer.write_state(state)

    # To transform string to datetime 
    def transform_datetime(self, this_dttm):
        """
        Convert string date to datetime object
        """
        
        with Transformer() as transformer:
            new_dttm = transformer._transform_datetime(this_dttm)
        return new_dttm

    # To process records
    def process_records(self, catalog, #pylint: disable=too-many-branches
                        stream_name,
                        records,
                        time_extracted,
                        bookmark_field=None,
                        max_bookmark_value=None,
                        last_datetime=None):
        """
        To process record in sync mode
        """
        stream = catalog.get_stream(stream_name)
        schema = stream.schema.to_dict()
        stream_metadata = metadata.to_map(stream.metadata)

        with metrics.record_counter(stream_name) as counter:
            for record in records:
                # Transform record for Singer.io
                with Transformer() as transformer:
                    transformed_record = transformer.transform(
                        record,
                        schema,
                        stream_metadata)

                    # Reset max_bookmark_value to new value if higher
                    if bookmark_field and (bookmark_field in transformed_record):
                        bookmark_date = transformed_record.get(bookmark_field)
                        bookmark_dttm = strptime_to_utc(bookmark_date)
                        last_dttm = strptime_to_utc(last_datetime)

                        if not max_bookmark_value:
                            max_bookmark_value = last_datetime

                        max_bookmark_dttm = strptime_to_utc(max_bookmark_value)

                        if bookmark_dttm > max_bookmark_dttm:
                            max_bookmark_value = strftime(bookmark_dttm)

                        # Keep only records whose bookmark is after the last_datetime
                        if bookmark_dttm >= last_dttm:
                            self.write_record(stream_name, transformed_record, \
                                time_extracted=time_extracted)
                            counter.increment()
                    else:
                        self.write_record(stream_name, transformed_record, time_extracted=time_extracted)
                        counter.increment()

            LOGGER.info('Stream: {}, Processed {} records'.format(stream_name, counter.value))
            return max_bookmark_value, counter.value

    # To reset minutes in local
    def remove_minutes_local(self, dttm, timezone):
        """
        To reset minutes to zero
        """
        new_dttm = dttm.astimezone(timezone).replace(
            minute=0, second=0, microsecond=0).astimezone(pytz.timezone('UTC')).strftime('%Y-%m-%dT%H:%M:%SZ')
        return new_dttm

    # To reset hours in local
    def remove_hours_local(self, dttm, timezone):
        """
        To reset hours to zero
        """
        new_dttm = dttm.astimezone(timezone).replace(
            hour=0, minute=0, second=0, microsecond=0).astimezone(pytz.timezone('UTC')).strftime('%Y-%m-%dT%H:%M:%SZ')
        return new_dttm

    # Sync a specific parent or child endpoint.
    def sync_endpoint(
            self,
            client,
            config,
            catalog,
            state,
            stream_name,
            stream_class,
            sync_streams,
            selected_streams,
            timezone_desc=None,
            parent_id=None):
        
        """
        To sync all streams (i.e. parent and child stream)
        """

        # endpoint_config variables
        base_path = stream_class.path or stream_name
        bookmark_field = next(iter(stream_class.replication_keys ), None)
        params = stream_class.params
        paging = stream_class.paging
        bookmark_query_field_from = stream_class.bookmark_query_field_from
        bookmark_query_field_to = stream_class.bookmark_query_field_to
        targeting_group = stream_class.targeting_group
        targeting_type = stream_class.targeting_type
        targeting_country_ind = stream_class.targeting_country_ind
        data_key_array = stream_class.data_key_array
        data_key_record = stream_class.data_key_record.format(targeting_type=targeting_type)
        id_fields = stream_class.key_properties
        parent = stream_class.parent
        date_window_size = int(stream_class.date_window_size)
        # Store parent_id into base_parent for bookmark writing
        base_parent = parent_id
        api_limit = int(config.get('page_size', 500)) # initially the 'limit' was 500

        # tap config variabless
        start_date = config.get('start_date')
        swipe_up_attribution_window = config.get('swipe_up_attribution_window', '28_DAY')
        view_attribution_window = config.get('view_attribution_window', '7_DAY')

        swipe_up_attr = int(swipe_up_attribution_window.replace('_DAY', ''))

        if view_attribution_window in ('1_HOUR', '3_HOUR', '6_HOUR',):
            view_attr = 1
        else:
            view_attr = int(view_attribution_window.replace('_DAY', ''))

        attribution_window = max(1, swipe_up_attr, view_attr)

        omit_empty = config.get('omit_empty', 'true')
        if '_stats_' in stream_name:
            params['omit_empty'] = omit_empty

        country_codes = config.get('targeting_country_codes', 'us').replace(' ', '').lower()
        if targeting_country_ind:
            country_code_list = country_codes.split(',')
        else:
            country_code_list = ['none']

        # Get the timezone and latest bookmark for the stream
        if not timezone_desc:
            timezone = pytz.timezone('UTC')
        else:
            timezone = pytz.timezone(timezone_desc)
        LOGGER.info('timezone = {}'.format(timezone))

        last_datetime = self.get_bookmark(state, stream_name, start_date, bookmark_field, parent, parent_id)
        max_bookmark_value = last_datetime

        # Convert to datetimes in local/ad account timezone
        now_datetime = utils.now()
        last_dttm = strptime_to_utc(last_datetime)

        report_granularity = params.get('granularity', 'HOUR')
        if '_stats_' in stream_name:
            LOGGER.info('report_granularity: {}'.format(report_granularity))

        if bookmark_query_field_from and bookmark_query_field_to:
            # date_window_size: Number of days in each date window
            # Set start window
            start_window = now_datetime - timedelta(days=attribution_window)
            if last_dttm < start_window:
                start_window = last_dttm
            # Set end window
            end_window = start_window + timedelta(days=date_window_size)

        else:
            start_window = last_dttm
            end_window = now_datetime
            diff_sec = (end_window - start_window).seconds
            date_window_size = math.ceil(diff_sec / (3600 * 24)) # round-up difference to days

        endpoint_total = 0
        total_records = 0

        while start_window < now_datetime:
            LOGGER.info('START Sync for Stream: {}{}'.format(
                stream_name,
                ', Date window from: {} to {}'.format(start_window.date(), end_window.date()) \
                    if bookmark_query_field_from else ''))

            if bookmark_query_field_from and bookmark_query_field_to:
                # Query parameter startDate and endDate must be in Eastern time zone
                # API will error if future dates are requested
                if report_granularity == 'DAY':
                    window_start_dt_str = self.remove_hours_local(start_window, timezone)
                    window_end_dt_str = self.remove_hours_local(end_window, timezone)
                    if window_start_dt_str == window_end_dt_str:
                        window_end_dt_str = self.remove_hours_local(end_window + timedelta(
                            days=1), timezone)
                else:
                    window_start_dt_str = self.remove_minutes_local(start_window, timezone)
                    window_end_dt_str = self.remove_minutes_local(end_window, timezone)
                    if window_start_dt_str == window_end_dt_str:
                        window_end_dt_str = self.remove_hours_local(end_window + timedelta(
                            hours=1), timezone)

                params[bookmark_query_field_from] = window_start_dt_str
                params[bookmark_query_field_to] = window_end_dt_str

            # This loop will run once for non-country_code endpoints
            #   and one or more times (for each country) for country_code endpoints
            for country_code in country_code_list:
                # Path
                if stream_name.startswith('targeting_'):
                    path = base_path.format(
                        targeting_group=targeting_group,
                        targeting_type=targeting_type,
                        country_code=country_code,
                        parent_id=parent_id)
                else:
                    path = base_path.format(
                        country_code=country_code,
                        parent_id=parent_id)

                # pagination: loop thru all pages of data using next (if not None)
                #   Reference: https://developers.snapchat.com/api/docs/#pagination
                total_records = 0
                offset = 1
                page = 1
                if paging:
                    limit = api_limit # Allowed values: 50 - 1000
                    params['limit'] = limit
                else:
                    limit = None

                for key, val in params.items():
                    # Replace variables in params
                    new_val = str(val).format(
                        swipe_up_attribution_window=swipe_up_attribution_window,
                        view_attribution_window=view_attribution_window)
                    params[key] = new_val
                # concate params
                querystring = '&'.join(['%s=%s' % (key, value) for (key, value) in params.items()])

                # initialize next_url
                next_url = '{}/{}?{}'.format(
                    client.base_url,
                    path,
                    querystring)

                # pagination loop
                while next_url is not None:

                    # API request data
                    data = {}
                    try:
                        data = client.get(
                            url=next_url,
                            endpoint=stream_name)
                    except Exception as err:
                        LOGGER.error('{}'.format(err))
                        LOGGER.error('URL for Stream {}: {}'.format(stream_name, next_url))
                        raise

                    # time_extracted: datetime when the data was extracted from the API
                    time_extracted = utils.now()
                    if not data or data is None or data == {}:
                        LOGGER.info('No data results returned')
                        total_records = 0
                        break # No data results

                    request_status = data.get('request_status')
                    if request_status != 'SUCCESS':
                        raise RuntimeError(data)

                    # Get pagination next_url
                    next_url = data.get('paging', {}).get('next_link', None)

                    # Transform data with transform_json from transform.py
                    # The data_key_array identifies the array/list of records below the <root> element
                    transformed_data = [] # initialize the record list

                    # Reports stats streams de-nesting
                    if '_stats_' in stream_name:
                        for data_record in data.get(data_key_array, []):
                            base_record = data_record.get(data_key_record, {})
                            records = base_record.get('timeseries', [])
                            for record in records:
                                # Add parent base_record fields to record
                                for key, val in base_record.items():
                                    if key not in ('start_time', 'end_time', 'timeseries'):
                                        record[key] = val

                                # De-nest stats
                                stats = record.get('stats', {})
                                for key, val in stats.items():
                                    record[key] = val
                                record.pop('stats', None)

                                # transform record
                                try:
                                    transformed_record = humps.decamelize(record)
                                except Exception as err:
                                    LOGGER.error('{}'.format(err))
                                    raise

                                # verify primary_keys are in tansformed_record
                                if 'id' not in transformed_record or 'start_time' not in transformed_record:
                                    LOGGER.error('Stream: {}, Missing key (id or start_time)'.format(
                                        stream_name))
                                    LOGGER.error('transformed_record: {}'.format(transformed_record))
                                    raise RuntimeError

                                transformed_data.append(transformed_record)
                                # End for record in records
                            # End for data_record in array
                        # End stats stream

                    # Other streams de-nesting
                    else: # Not stats stream
                        for data_record in data.get(data_key_array, []):
                            sub_request_status = data_record.get('sub_request_status')
                            if sub_request_status != 'SUCCESS':
                                raise RuntimeError(data_record)

                            record = data_record.get(data_key_record, {})

                            # Transforms to align schemas for targeting streams
                            if stream_name.startswith('targeting_'):
                                record['targeting_group'] = targeting_group
                                record['targeting_type'] = targeting_type
                                if country_code != 'none':
                                    record['country_code'] = country_code
                                if targeting_group == 'geo':
                                    record_id = record.get(targeting_type, {}).get('id')
                                    record_name = record.get(targeting_type, {}).get('name')
                                    record['id'] = record_id
                                    record['name'] = record_name
                                if targeting_type == 'postal_code':
                                    record_id = record.get('postalCode')
                                    record['id'] = record_id
                                    record['name'] = record_id
                                    record.pop('postalCode')

                            # Add parent id field/value
                            if parent and parent_id:
                                parent_key = '{}_id'.format(parent)
                                record[parent_key] = parent_id

                            # transform record (remove inconsistent use of CamelCase)
                            try:
                                transformed_record = humps.decamelize(record)
                            except Exception as err:
                                LOGGER.error('{}'.format(err))
                                LOGGER.error('error record: {}'.format(record))
                                raise

                            # verify primary_keys are in tansformed_record
                            for key in id_fields:
                                if not transformed_record.get(key):
                                    LOGGER.error('Stream: {}, Missing key {}'.format(
                                        stream_name, key))
                                    LOGGER.info('transformed_record: {}'.format(transformed_record))
                                    raise RuntimeError

                            transformed_data.append(transformed_record)
                            # End for data_record in array
                        # End non-stats stream

                    if not transformed_data or transformed_data is None:
                        LOGGER.info('No transformed data for data = {}'.format(data))
                        total_records = 0
                        break # No transformed_data results

                    # Process records and get the max_bookmark_value and record_count if stream is selected in catalog
                    record_count = 0
                    if stream_name in selected_streams and stream_name in sync_streams:
                        max_bookmark_value, record_count = self.process_records(
                            catalog=catalog,
                            stream_name=stream_name,
                            records=transformed_data,
                            time_extracted=time_extracted,
                            bookmark_field=bookmark_field,
                            max_bookmark_value=max_bookmark_value,
                            last_datetime=last_datetime)
                        LOGGER.info('Stream {}, batch processed {} records'.format(
                            stream_name, record_count))

                    # Loop thru parent batch records for each children objects (if should stream)
                    children = stream_class.children
                    if children:
                        for child_stream_name in children:
                            if child_stream_name in sync_streams:
                                LOGGER.info('START Syncing: {}'.format(child_stream_name))
                                self.write_schema(catalog, child_stream_name, sync_streams, selected_streams)
                                # For each parent record
                                for record in transformed_data:
                                    i = 0
                                    # Set parent_id
                                    for id_field in id_fields:
                                        if i == 0:
                                            parent_id_field = id_field
                                        if id_field == 'id':
                                            parent_id_field = id_field
                                        i = i + 1
                                    parent_id = record.get(parent_id_field)

                                    if stream_name == 'ad_accounts':
                                        timezone_desc = record.get('timezone', timezone_desc)

                                    # set currently syncing as child stream
                                    update_currently_syncing(state, child_stream_name)
                                    # sync_endpoint for child
                                    LOGGER.info(
                                        'START Sync for Stream: {}, parent_stream: {}, parent_id: {}'\
                                            .format(child_stream_name, stream_name, parent_id))

                                    child_total_records = self.sync_endpoint(
                                        client=client,
                                        config=config,
                                        catalog=catalog,
                                        state=state,
                                        stream_name=child_stream_name,
                                        stream_class=STREAMS[child_stream_name],
                                        sync_streams=sync_streams,
                                        selected_streams=selected_streams,
                                        timezone_desc=timezone_desc,
                                        parent_id=parent_id)

                                    LOGGER.info(
                                        'FINISHED Sync for Stream: {}, parent_id: {}, total_records: {}'\
                                            .format(child_stream_name, parent_id, child_total_records))
                                    # End transformed data record loop
                                # End if child in sync_streams
                            # End child streams for parent
                        # End if children

                    # Parent record batch
                    total_records = total_records + record_count
                    endpoint_total = endpoint_total + record_count

                    LOGGER.info('Synced Stream: {}, page: {}, records: {} to {}'.format(
                        stream_name,
                        page,
                        offset,
                        total_records))
                    # Pagination: increment the offset by the limit (batch-size) and page
                    if limit:
                        offset = offset + limit
                    page = page + 1
                    # End page/batch - while next URL loop
                # End country_code loop

            # Update the state with the max_bookmark_value for the stream date window
            # Snapchat Ads API does not allow page/batch sorting; bookmark written for date window
            # If current stream has children then parent_id value will be changed, so we are using base_parent for current stream's bookmark writing
            if bookmark_field and stream_name in selected_streams:
                self.write_bookmark(state, stream_name, max_bookmark_value, bookmark_field, parent, base_parent)

            # Increment date window and sum endpoint_total
            start_window = end_window
            next_end_window = end_window + timedelta(days=date_window_size)
            if next_end_window > now_datetime:
                end_window = now_datetime
            else:
                end_window = next_end_window
            # End date window

        # Return total_records (for all pages and date windows)
        return endpoint_total

# Reference: https://developers.snapchat.com/api/docs/#get-all-organizations
class Organizations(SnapchatAds):
    tap_stream_id = 'organizations'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_keys = ['updated_at']
    path = 'me/organizations'
    data_key_array = 'organizations'
    data_key_record = 'organization'
    paging = False
    params = {}
    children = ['funding_sources', 'billing_centers', 'members', 'roles', 'ad_accounts', 'product_catalogs']
    
# Reference: https://developers.snapchat.com/api/docs/?python#funding-sources
class FundingSources(SnapchatAds):
    tap_stream_id = 'funding_sources'
    parent_stream = 'organizations'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_keys = ['updated_at']
    path = 'organizations/{parent_id}/fundingsources'
    data_key_array = 'fundingsources'
    data_key_record = 'fundingsource'
    paging = False
    parent = 'organization'
    params = {}

# Reference: https://developers.snapchat.com/api/docs/?python#get-all-billing-centers
class BillingCenters(SnapchatAds):
    tap_stream_id = 'billing_centers'
    parent_stream = 'organizations'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_keys = ['updated_at']
    path = 'organizations/{parent_id}/billingcenters'
    data_key_array = 'billingcenters'
    data_key_record = 'billingcenter'
    paging = True
    parent = 'organization'
    params = {}
    
# Reference: https://developers.snapchat.com/api/docs/?python#get-all-members-of-an-organization
class Members(SnapchatAds):
    tap_stream_id = 'members'
    parent_stream = 'organizations'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_keys = ['updated_at']
    path = 'organizations/{parent_id}/members'
    data_key_array = 'members'
    data_key_record = 'member'
    paging = False
    parent = 'organization'
    params = {}
                
# Reference: https://developers.snapchat.com/api/docs/?python#get-all-roles-in-organization
class Roles(SnapchatAds):
    tap_stream_id = 'roles'
    parent_stream = 'organizations'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    path = 'organizations/{parent_id}/roles'
    data_key_array = 'roles'
    data_key_record = 'role'
    paging = True
    parent = 'organization'
    params = {}
    
# Reference: https://developers.snapchat.com/api/docs/#get-all-ad-accounts
class AdAccounts(SnapchatAds):
    tap_stream_id = 'ad_accounts'
    parent_stream = 'organizations'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_keys = ['updated_at']
    path = 'organizations/{parent_id}/adaccounts'
    data_key_array = 'adaccounts'
    data_key_record = 'adaccount'
    paging = False
    parent = 'organization'
    params = {}
    children = ['ad_account_stats_daily', 'ad_account_stats_hourly', 'audience_segments', 'pixels', 'media', 'creatives', 'phone_numbers', 'campaigns', 'ad_squads', 'ads']
    
# Reference: https://developers.snapchat.com/api/docs/#get-ad-account-stats
class AdAccountStatsDaily(SnapchatAds):
    tap_stream_id = 'ad_account_stats_daily'
    grandparent_stream = 'organizations'
    parent_stream = 'ad_accounts'
    json_schema = 'schemas/shared/ad_account_stats.json'
    key_properties = ['id', 'start_time']
    replication_method = 'INCREMENTAL' # query filtered
    replication_keys = ['end_time']
    bookmark_query_field_from = 'start_time'
    bookmark_query_field_to = 'end_time'
    path= 'adaccounts/{parent_id}/stats'
    data_key_array = 'timeseries_stats'
    data_key_record = 'timeseries_stat'
    date_window_size = 30
    paging = False
    parent = 'ad_account'
    params = {
        'fields': 'spend',
        'granularity': 'DAY',
        'omit_empty': 'false',
        'conversion_source_types': 'web,app,total',
        'swipe_up_attribution_window': '{swipe_up_attribution_window}',
        'view_attribution_window': '{view_attribution_window}'
    }

# Reference: https://developers.snapchat.com/api/docs/#get-ad-account-stats
class AdAccountStatsHourly(SnapchatAds):
    tap_stream_id = 'ad_account_stats_hourly'
    grandparent_stream = 'organizations'
    parent_stream = 'ad_accounts'
    json_schema = 'schemas/shared/ad_account_stats.json'
    key_properties = ['id', 'start_time']
    replication_method = 'INCREMENTAL' # query filtered
    replication_keys = ['end_time']
    bookmark_query_field_from = 'start_time'
    bookmark_query_field_to = 'end_time'
    path = 'adaccounts/{parent_id}/stats'
    data_key_array = 'timeseries_stats'
    data_key_record = 'timeseries_stat'
    date_window_size = 7
    paging = False
    parent = 'ad_account'
    params = {
        'fields': 'spend',
        'granularity': 'HOUR',
        'omit_empty': 'false',
        'conversion_source_types': 'web,app,total',
        'swipe_up_attribution_window': '{swipe_up_attribution_window}',
        'view_attribution_window': '{view_attribution_window}'
    }

# Reference: https://developers.snapchat.com/api/docs/#get-all-audience-segments
class AudienceSegments(SnapchatAds):
    tap_stream_id = 'audience_segments'
    grandparent_stream = 'organizations'
    parent_stream = 'ad_accounts'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_keys = ['updated_at']
    path = 'adaccounts/{parent_id}/segments'
    data_key_array = 'segments'
    data_key_record = 'segment'
    paging = True
    parent = 'ad_account'
    params = {}
    
# Reference: https://developers.snapchat.com/api/docs/#get-the-pixel-associated-with-an-ad-account
class Pixels(SnapchatAds):
    tap_stream_id = 'pixels'
    grandparent_stream = 'organizations'
    parent_stream = 'ad_accounts'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_keys = ['updated_at']
    path = 'adaccounts/{parent_id}/pixels'
    data_key_array = 'pixels'
    data_key_record = 'pixel'
    paging = True
    parent = 'ad_account'
    params = {}
    children = ['pixel_domain_stats']
    
# Reference: https://developers.snapchat.com/api/docs/#get-pixel-domains
class PixelDomainStats(SnapchatAds):
    tap_stream_id = 'pixel_domain_stats'
    great_grandparent_stream = 'organizations'
    grandparent_stream = 'ad_accounts'
    parent_stream = 'pixels'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    path = 'pixels/{parent_id}/domains/stats'
    data_key_array = 'timeseries_stats'
    data_key_record = 'timeseries_stat'
    paging = False
    parent = 'pixel'
    params = {}
    
# Reference: https://developers.snapchat.com/api/docs/#get-all-media
class Media(SnapchatAds):
    tap_stream_id = 'media'
    grandparent_stream = 'organizations'
    parent_stream = 'ad_accounts'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_keys = ['updated_at']
    path = 'adaccounts/{parent_id}/media'
    data_key_array = 'media'
    data_key_record = 'media'
    paging = True
    parent = 'ad_account'
    params = {}
    
# Reference: https://developers.snapchat.com/api/docs/#get-all-creatives
class Creatives(SnapchatAds):
    tap_stream_id = 'creatives'
    grandparent_stream = 'organizations'
    parent_stream = 'ad_accounts'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_keys = ['updated_at']
    path = 'adaccounts/{parent_id}/creatives'
    data_key_array = 'creatives'
    data_key_record = 'creative'
    paging = True
    parent = 'ad_account'
    params = {}
                        
# Reference: https://developers.snapchat.com/api/docs/#swipe-to-call-text-phone-number-verification
class PhoneNumbers(SnapchatAds):
    tap_stream_id = 'phone_numbers'
    grandparent_stream = 'organizations'
    parent_stream = 'ad_accounts'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_keys = ['updated_at']
    path = 'adaccounts/{parent_id}/phone_numbers'
    data_key_array = 'phone_numbers'
    data_key_record = 'phone_number'
    paging = True
    parent = 'ad_account'
    params = {}

# Reference: https://developers.snapchat.com/api/docs/#get-all-campaigns
class Campaigns(SnapchatAds):
    tap_stream_id = 'campaigns'
    grandparent_stream = 'organizations'
    parent_stream = 'ad_accounts'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_keys = ['updated_at']
    path = 'adaccounts/{parent_id}/campaigns'
    data_key_array = 'campaigns'
    data_key_record = 'campaign'
    paging = True
    parent = 'ad_account'
    params = {}
    children = ['campaign_stats_daily', 'campaign_stats_hourly']
    
# Reference: https://developers.snapchat.com/api/docs/#get-campaign-stats
class CampaignStatsDaily(SnapchatAds):
    tap_stream_id = 'campaign_stats_daily'
    great_grandparent_stream = 'organizations'
    grandparent_stream = 'ad_accounts'
    parent_stream = 'campaigns'
    json_schema = 'schemas/shared/stats.json'
    key_properties = ['id', 'start_time']
    replication_method = 'INCREMENTAL' # query filtered
    replication_keys = ['end_time']
    bookmark_query_field_from = 'start_time'
    bookmark_query_field_to = 'end_time'
    path = 'campaigns/{parent_id}/stats'
    data_key_array = 'timeseries_stats'
    data_key_record = 'timeseries_stat'
    date_window_size = 30
    paging = False
    parent = 'campaign'
    params = {
        'fields': ALL_STATS_FIELDS,
        'granularity': 'DAY',
        'omit_empty': 'false',
        'conversion_source_types': 'web,app,total',
        'swipe_up_attribution_window': '{swipe_up_attribution_window}',
        'view_attribution_window': '{view_attribution_window}'
    }

# Reference: https://developers.snapchat.com/api/docs/#get-campaign-stats
class CampaignStatsHourly(SnapchatAds):
    tap_stream_id = 'campaign_stats_hourly'
    great_grandparent_stream = 'organizations'
    grandparent_stream = 'ad_accounts'
    parent_stream = 'campaigns'
    json_schema = 'schemas/shared/stats.json'
    key_properties = ['id', 'start_time']
    replication_method = 'INCREMENTAL' # query filtered
    replication_keys = ['end_time']
    bookmark_query_field_from = 'start_time'
    bookmark_query_field_to = 'end_time'
    path = 'campaigns/{parent_id}/stats'
    data_key_array = 'timeseries_stats'
    data_key_record = 'timeseries_stat'
    date_window_size = 7
    paging = False
    parent = 'campaign'
    params = {
        'fields': ALL_STATS_FIELDS,
        'granularity': 'HOUR',
        'omit_empty': 'false',
        'conversion_source_types': 'web,app,total',
        'swipe_up_attribution_window': '{swipe_up_attribution_window}',
        'view_attribution_window': '{view_attribution_window}'
    }
    
# Reference: https://developers.snapchat.com/api/docs/#get-all-ad-squads-under-an-ad-account
class AdSquads(SnapchatAds):
    tap_stream_id = 'ad_squads'
    grandparent_stream = 'organizations'
    parent_stream = 'ad_accounts'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_keys = ['updated_at']
    path = 'adaccounts/{parent_id}/adsquads'
    data_key_array = 'adsquads'
    data_key_record = 'adsquad'
    paging = True
    parent = 'ad_account'
    params = {}
    children = ['ad_squad_stats_daily', 'ad_squad_stats_hourly']
    
# Reference: https://developers.snapchat.com/api/docs/#get-ad-squad-stats
class AdSquadStatsDaily(SnapchatAds):
    tap_stream_id = 'ad_squad_stats_daily'
    great_grandparent_stream = 'organizations'
    grandparent_stream = 'ad_accounts'
    parent_stream = 'ad_squads'
    json_schema = 'schemas/shared/stats.json'
    key_properties = ['id', 'start_time']
    replication_method = 'INCREMENTAL' # query filtered
    replication_keys = ['end_time']
    bookmark_query_field_from = 'start_time'
    bookmark_query_field_to = 'end_time'
    path = 'adsquads/{parent_id}/stats'
    data_key_array = 'timeseries_stats'
    data_key_record = 'timeseries_stat'
    date_window_size = 30
    paging = False
    parent = 'ad_squad'
    params = {
        'fields': ALL_STATS_FIELDS,
        'granularity': 'DAY',
        'omit_empty': 'false',
        'conversion_source_types': 'web,app,total',
        'swipe_up_attribution_window': '{swipe_up_attribution_window}',
        'view_attribution_window': '{view_attribution_window}'
    }

# Reference: https://developers.snapchat.com/api/docs/#get-ad-squad-stats
class AdSquadStatsHourly(SnapchatAds):
    tap_stream_id = 'ad_squad_stats_hourly'
    great_grandparent_stream = 'organizations'
    grandparent_stream = 'ad_accounts'
    parent_stream = 'ad_squads'
    json_schema = 'schemas/shared/stats.json'
    key_properties = ['id', 'start_time']
    replication_method = 'INCREMENTAL' # query filtered
    replication_keys = ['end_time']
    bookmark_query_field_from = 'start_time'
    bookmark_query_field_to = 'end_time'
    path = 'adsquads/{parent_id}/stats'
    data_key_array = 'timeseries_stats'
    data_key_record = 'timeseries_stat'
    date_window_size = 7
    paging = False
    parent = 'ad_squad'
    params = {
        'fields': ALL_STATS_FIELDS,
        'granularity': 'HOUR',
        'omit_empty': 'false',
        'conversion_source_types': 'web,app,total',
        'swipe_up_attribution_window': '{swipe_up_attribution_window}',
        'view_attribution_window': '{view_attribution_window}'
    }
    
# Reference: https://developers.snapchat.com/api/docs/#get-all-ads-under-an-ad-account
class Ads(SnapchatAds):
    tap_stream_id = 'ads'
    grandparent_stream = 'organizations'
    parent_stream = 'ad_accounts'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_keys = ['updated_at']
    path = 'adaccounts/{parent_id}/ads'
    data_key_array = 'ads'
    data_key_record = 'ad'
    paging = True
    parent = 'ad_account'
    params = {}
    children = ['ad_stats_daily', 'ad_stats_hourly']
    
# Reference: https://developers.snapchat.com/api/docs/#get-ad-stats
class AdStatsDaily(SnapchatAds):
    tap_stream_id = 'ad_stats_daily'
    great_grandparent_stream = 'organizations'
    grandparent_stream = 'ad_accounts'
    parent_stream = 'ads'
    json_schema = 'schemas/shared/stats.json'
    key_properties = ['id', 'start_time']
    replication_method = 'INCREMENTAL' # query filtered
    replication_keys = ['end_time']
    bookmark_query_field_from = 'start_time'
    bookmark_query_field_to = 'end_time'
    path = 'ads/{parent_id}/stats'
    data_key_array = 'timeseries_stats'
    data_key_record = 'timeseries_stat'
    date_window_size = 30
    paging = False
    parent = 'ad'
    params = {
        'fields': ALL_STATS_FIELDS,
        'granularity': 'DAY',
        'omit_empty': 'false',
        'conversion_source_types': 'web,app,total',
        'swipe_up_attribution_window': '{swipe_up_attribution_window}',
        'view_attribution_window': '{view_attribution_window}'
    }

# Reference: https://developers.snapchat.com/api/docs/#get-ad-stats
class AdStatsHourly(SnapchatAds):
    tap_stream_id = 'ad_stats_hourly'
    great_grandparent_stream = 'organizations'
    grandparent_stream = 'ad_accounts'
    parent_stream = 'ads'
    json_schema = 'schemas/shared/stats.json'
    key_properties = ['id', 'start_time']
    replication_method = 'INCREMENTAL' # query filtered
    replication_keys = ['end_time']
    bookmark_query_field_from = 'start_time'
    bookmark_query_field_to = 'end_time'
    path = 'ads/{parent_id}/stats'
    data_key_array = 'timeseries_stats'
    data_key_record = 'timeseries_stat'
    date_window_size = 7
    paging = False
    parent = 'ad'
    params = {
        'fields': ALL_STATS_FIELDS,
        'granularity': 'HOUR',
        'omit_empty': 'false',
        'conversion_source_types': 'web,app,total',
        'swipe_up_attribution_window': '{swipe_up_attribution_window}',
        'view_attribution_window': '{view_attribution_window}'
    }
    
# Reference: https://developers.snapchat.com/api/docs/#get-all-catalogs
class ProductCatalogs(SnapchatAds):
    tap_stream_id = 'product_catalogs'
    parent_stream = 'organizations'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_keys = ['updated_at']
    path = 'organizations/{parent_id}/catalogs'
    data_key_array = 'catalogs'
    data_key_record = 'catalog'
    paging = True
    parent = 'organization'
    params = {}
    children = ['product_sets']
    
# Reference: https://developers.snapchat.com/api/docs/#get-all-product-sets
class ProductSets(SnapchatAds):
    tap_stream_id = 'product_sets'
    grandparent_stream = 'organizations'
    parent_stream = 'product_catalogs'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    path = 'catalogs/{parent_id}/product_sets'
    data_key_array = 'product_sets'
    data_key_record = 'product_set'
    paging = True
    parent = 'product_catalog'
    params = {}

# DEMO - AGE GROUP
# Reference: https://developers.snapchat.com/api/docs/#demographics-age-groups
class TargetingAgeGroups(SnapchatAds):
    tap_stream_id = 'targeting_age_groups'
    json_schema = 'schemas/shared/targeting.json'
    targeting_group = 'demographics'
    targeting_type = 'age_group'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    path = 'targeting/{targeting_group}/{targeting_type}'
    data_key_array = 'targeting_dimensions'
    data_key_record = '{targeting_type}'
    paging = True
    params = {}

# DEMO - GENDER
# Reference: https://developers.snapchat.com/api/docs/#demographics-gender
class TargetingGenders(SnapchatAds):
    tap_stream_id = 'targeting_genders'
    json_schema = 'schemas/shared/targeting.json'
    targeting_group = 'demographics'
    targeting_type = 'gender'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    path = 'targeting/{targeting_group}/{targeting_type}'
    data_key_array = 'targeting_dimensions'
    data_key_record = '{targeting_type}'
    paging = True
    params = {}
        
# DEMO - LANGUAGES
# Reference: https://developers.snapchat.com/api/docs/#demographics-language
class TargetingLanguages(SnapchatAds):
    tap_stream_id = 'targeting_languages'
    json_schema = 'schemas/shared/targeting.json'
    targeting_group = 'demographics'
    targeting_type = 'languages'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    path = 'targeting/{targeting_group}/{targeting_type}'
    data_key_array = 'targeting_dimensions'
    data_key_record = '{targeting_type}'
    paging = True
    params = {}
    
# DEMO - ADVANCED DEMOGRAPHICS
# Reference: https://developers.snapchat.com/api/docs/#demographics-advanced-demographics
class TargetingAdvancedDemographics(SnapchatAds):
    tap_stream_id = 'targeting_advanced_demographics'
    json_schema = 'schemas/shared/targeting.json'
    targeting_group = 'demographics'
    targeting_type = 'advanced_demographics'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    path = 'targeting/{targeting_group}/{targeting_type}'
    data_key_array = 'targeting_dimensions'
    data_key_record = '{targeting_type}'
    paging = True
    params = {}
    
# DEVICE - CONNECTION TYPE
# Reference: https://developers.snapchat.com/api/docs/#device-connection-type
class TargetingConnectionTypes(SnapchatAds):
    tap_stream_id = 'targeting_connection_types'
    json_schema = 'schemas/shared/targeting.json'
    targeting_group = 'device'
    targeting_type = 'connection_type'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    path = 'targeting/{targeting_group}/{targeting_type}'
    data_key_array = 'targeting_dimensions'
    data_key_record = '{targeting_type}'
    paging = True
    params = {}

# DEVICE - OS TYPE
# Reference: https://developers.snapchat.com/api/docs/#device-os-type
class TargetingOSTypes(SnapchatAds):
    tap_stream_id = 'targeting_os_types'
    json_schema = 'schemas/shared/targeting.json'
    targeting_group = 'device'
    targeting_type = 'os_type'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    path = 'targeting/{targeting_group}/{targeting_type}'
    data_key_array = 'targeting_dimensions'
    data_key_record = '{targeting_type}'
    paging = True
    params = {}
    
# DEVICE - OS VERSION - iOS
# Reference: https://developers.snapchat.com/api/docs/#device-os-version
class TargetingIOSVersions(SnapchatAds):
    tap_stream_id = 'targeting_ios_versions'
    json_schema = 'schemas/shared/targeting.json'
    targeting_group = 'device'
    targeting_type = 'os_version'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    path = 'targeting/device/iOS/os_version'
    data_key_array = 'targeting_dimensions'
    data_key_record = '{targeting_type}'
    paging = True
    params = {}
        
# DEVICE - OS VERSION - ANDROID
# Reference: https://developers.snapchat.com/api/docs/#device-os-version
class TargetingAndroidVersions(SnapchatAds):
    tap_stream_id = 'targeting_android_versions'
    json_schema = 'schemas/shared/targeting.json'
    targeting_group = 'device'
    targeting_type = 'os_version'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    path = 'targeting/device/ANDROID/os_version'
    data_key_array = 'targeting_dimensions'
    data_key_record = '{targeting_type}'
    paging = True
    params = {}

# DEVICE - CARRIER
# Reference: https://developers.snapchat.com/api/docs/#device-carrier
class TargetingCarriers(SnapchatAds):
    tap_stream_id = 'targeting_carriers'
    json_schema = 'schemas/shared/targeting.json'
    targeting_group = 'device'
    targeting_type = 'carrier'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    path = 'targeting/{targeting_group}/{targeting_type}'
    data_key_array = 'targeting_dimensions'
    data_key_record = '{targeting_type}'
    paging = True
    params = {}
        
# DEVICE - MARKETING NAME
# Reference: https://developers.snapchat.com/api/docs/#device-make
class TargetingDeviceMakes(SnapchatAds):
    tap_stream_id = 'targeting_device_makes'
    json_schema = 'schemas/shared/targeting.json'
    targeting_group = 'device'
    targeting_type = 'marketing_name'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    path = 'targeting/{targeting_group}/{targeting_type}'
    data_key_array = 'targeting_dimensions'
    data_key_record = '{targeting_type}'
    paging = True
    params = {}

# GEO - COUNTRY
# Reference: https://developers.snapchat.com/api/docs/#geolocation-country
class TargetingCountries(SnapchatAds):
    tap_stream_id = 'targeting_countries'
    json_schema = 'schemas/shared/targeting_geo.json'
    targeting_group = 'geo'
    targeting_type = 'country'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    path = 'targeting/{targeting_group}/{targeting_type}'
    data_key_array = 'targeting_dimensions'
    data_key_record = '{targeting_type}'
    paging = True
    params = {}

# GEO - REGION
# Reference: https://developers.snapchat.com/api/docs/#geolocation-region-state
class TargetingRegions(SnapchatAds):
    tap_stream_id = 'targeting_regions'
    json_schema = 'schemas/shared/targeting_geo.json'
    targeting_group = 'geo'
    targeting_type = 'region'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    path = 'targeting/geo/{country_code}/region'
    targeting_country_ind = True
    data_key_array = 'targeting_dimensions'
    data_key_record = '{targeting_type}'
    api_method = 'GET'
    paging = True
    params = {}
    
# GEO - METRO
# Reference: https://developers.snapchat.com/api/docs/#geolocation-metro-dma
class TargetingMetros(SnapchatAds):
    tap_stream_id = 'targeting_metros'
    json_schema = 'schemas/shared/targeting_geo.json'
    targeting_group = 'geo'
    targeting_type = 'metro'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    path = 'targeting/geo/{country_code}/metro'
    targeting_country_ind = True
    data_key_array = 'targeting_dimensions'
    data_key_record = '{targeting_type}'
    api_method = 'GET'
    paging = True
    params = {}

# GEO - POSTAL CODE
# Reference: https://developers.snapchat.com/api/docs/#zipcode
class TargetingPostalCodes(SnapchatAds):
    tap_stream_id = 'targeting_postal_codes'
    json_schema = 'schemas/shared/targeting_geo.json'
    targeting_group = 'geo'
    targeting_type = 'postal_code'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    path = 'targeting/geo/{country_code}/postal_code'
    targeting_country_ind = True
    data_key_array = 'targeting_dimensions'
    data_key_record = '{targeting_type}'
    api_method = 'GET'
    paging = True
    params = {}
    
# INTERESTS - SCLS (Snap Lifestyle Categories)
# Reference: https://developers.snapchat.com/api/docs/#interests-snap-lifestyle-categories
class TargetingInterestsSCLS(SnapchatAds):
    tap_stream_id = 'targeting_interests_scls'
    json_schema = 'schemas/shared/targeting.json'
    targeting_group = 'interests'
    targeting_type = 'scls'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    path = 'targeting/{targeting_group}/{targeting_type}'
    data_key_array = 'targeting_dimensions'
    data_key_record = '{targeting_type}'
    api_method = 'GET'
    paging = True
    params = {}

# INTERESTS - DLXS (Oracle Datalogix)
# Reference: https://developers.snapchat.com/api/docs/#get-oracle-datalogix-dlxs-interest-targeting-options
class TargetingInterestsDLXS(SnapchatAds):
    tap_stream_id = 'targeting_interests_dlxs'
    json_schema = 'schemas/shared/targeting.json'
    targeting_group = 'interests'
    targeting_type = 'dlxs'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    path = 'targeting/{targeting_group}/{targeting_type}'
    data_key_array = 'targeting_dimensions'
    data_key_record = '{targeting_type}'
    api_method = 'GET'
    paging = True
    params = {}
        
# INTERESTS - DLXC (Oracle Datalogix)
# Reference: https://developers.snapchat.com/api/docs/#get-oracle-datalogix-dlxc-interest-targeting-options
class TargetingInterestsDLXC(SnapchatAds):
    tap_stream_id = 'targeting_interests_dlxc'
    json_schema = 'schemas/shared/targeting.json'
    targeting_group = 'interests'
    targeting_type = 'dlxc'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    path = 'targeting/{targeting_group}/{targeting_type}'
    data_key_array = 'targeting_dimensions'
    data_key_record = '{targeting_type}'
    api_method = 'GET'
    paging = True
    params = {}
    
# INTERESTS - DLXP (Oracle Datalogix)
# Reference: https://developers.snapchat.com/api/docs/#get-oracle-datalogix-dlxp-interest-targeting-options
class TargetingInterestsDLXP(SnapchatAds):
    tap_stream_id = 'targeting_interests_dlxp'
    json_schema = 'schemas/shared/targeting.json'
    targeting_group = 'interests'
    targeting_type = 'dlxp'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    path = 'targeting/{targeting_group}/{targeting_type}'
    data_key_array = 'targeting_dimensions'
    data_key_record = '{targeting_type}'
    api_method = 'GET'
    paging = True
    params = {}
    
# INTERESTS - NLN (Nielsen)
# Reference: https://developers.snapchat.com/api/docs/#get-nielsen-interest-targeting-options
class TargetingInterestsNLN(SnapchatAds):
    tap_stream_id = 'targeting_interests_nln'
    json_schema = 'schemas/shared/targeting.json'
    targeting_group = 'interests'
    targeting_type = 'nln'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    path = 'targeting/{targeting_group}/{targeting_type}'
    data_key_array = 'targeting_dimensions'
    data_key_record = '{targeting_type}'
    api_method = 'GET'
    paging = True
    params = {}

# INTERESTS - PLC (Placed Visitation Segments)
# Reference: https://developers.snapchat.com/api/docs/#interests-placed-visitation-segments
class TargetingInterestsPLC(SnapchatAds):
    tap_stream_id = 'targeting_interests_plc'
    json_schema = 'schemas/shared/targeting.json'
    targeting_group = 'interests'
    targeting_type = 'plc'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    path = 'targeting/{targeting_group}/{targeting_type}'
    data_key_array = 'targeting_dimensions'
    data_key_record = '{targeting_type}'
    api_method = 'GET'
    paging = True
    params = {}

# LOCATION - Categories LOI
# Reference: https://developers.snapchat.com/api/docs/#location
class TargetingLocationCategories(SnapchatAds):
    tap_stream_id = 'targeting_location_categories'
    json_schema ='schemas/shared/targeting.json'
    targeting_group ='location'
    targeting_type ='categories_loi'
    key_properties =['id']
    replication_method ='FULL_TABLE'
    path ='targeting/{targeting_group}/{targeting_type}'
    data_key_array ='targeting_dimensions'
    data_key_record ='{targeting_type}'
    api_method ='GET'
    paging =True
    params ={}
    
# Root level streams
ROOT_STREAMS = {
    'organizations': Organizations,
    'targeting_age_groups': TargetingAgeGroups,
    'targeting_genders': TargetingGenders,
    'targeting_languages': TargetingLanguages,
    'targeting_advanced_demographics': TargetingAdvancedDemographics,
    'targeting_connection_types': TargetingConnectionTypes,
    'targeting_os_types': TargetingOSTypes,
    'targeting_ios_versions': TargetingIOSVersions,
    'targeting_android_versions': TargetingAndroidVersions,
    'targeting_carriers': TargetingCarriers,
    'targeting_device_makes': TargetingDeviceMakes,
    'targeting_countries': TargetingCountries,
    'targeting_regions': TargetingRegions,
    'targeting_metros': TargetingMetros,
    'targeting_postal_codes': TargetingPostalCodes,
    'targeting_interests_scls': TargetingInterestsSCLS,
    'targeting_interests_dlxs': TargetingInterestsDLXS,
    'targeting_interests_dlxc': TargetingInterestsDLXC,
    'targeting_interests_dlxp': TargetingInterestsDLXP,
    'targeting_interests_nln': TargetingInterestsNLN,
    'targeting_interests_plc': TargetingInterestsPLC,
    'targeting_location_categories': TargetingLocationCategories
}

# All the streams
STREAMS = {
    'organizations': Organizations,
    'funding_sources': FundingSources,
    'billing_centers': BillingCenters,
    'members': Members,
    'roles': Roles,
    'ad_accounts': AdAccounts,
    'ad_account_stats_daily': AdAccountStatsDaily,
    'ad_account_stats_hourly': AdAccountStatsHourly,
    'audience_segments': AudienceSegments,
    'pixels': Pixels,
    'pixel_domain_stats': PixelDomainStats,
    'media': Media,
    'creatives': Creatives,
    'phone_numbers': PhoneNumbers,
    'campaigns': Campaigns,
    'campaign_stats_daily': CampaignStatsDaily,
    'campaign_stats_hourly': CampaignStatsHourly,
    'ad_squads': AdSquads,
    'ad_squad_stats_daily': AdSquadStatsDaily,
    'ad_squad_stats_hourly': AdSquadStatsHourly,
    'ads': Ads,
    'ad_stats_daily': AdStatsDaily,
    'ad_stats_hourly': AdStatsHourly,
    'product_catalogs': ProductCatalogs,
    'product_sets': ProductSets,
    'targeting_age_groups': TargetingAgeGroups,
    'targeting_genders': TargetingGenders,
    'targeting_languages': TargetingLanguages,
    'targeting_advanced_demographics': TargetingAdvancedDemographics,
    'targeting_connection_types': TargetingConnectionTypes,
    'targeting_os_types': TargetingOSTypes,
    'targeting_ios_versions': TargetingIOSVersions,
    'targeting_android_versions': TargetingAndroidVersions,
    'targeting_carriers': TargetingCarriers,
    'targeting_device_makes': TargetingDeviceMakes,
    'targeting_countries': TargetingCountries,
    'targeting_regions': TargetingRegions,
    'targeting_metros': TargetingMetros,
    'targeting_postal_codes': TargetingPostalCodes,
    'targeting_interests_scls': TargetingInterestsSCLS,
    'targeting_interests_dlxs': TargetingInterestsDLXS,
    'targeting_interests_dlxc': TargetingInterestsDLXC,
    'targeting_interests_dlxp': TargetingInterestsDLXP,
    'targeting_interests_nln': TargetingInterestsNLN,
    'targeting_interests_plc': TargetingInterestsPLC,
    'targeting_location_categories': TargetingLocationCategories
}