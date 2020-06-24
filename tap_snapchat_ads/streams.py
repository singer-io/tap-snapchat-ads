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
ALL_STATS_FIELDS = 'android_installs,attachment_avg_view_time_millis,attachment_impressions,attachment_quartile_1,attachment_quartile_2,attachment_quartile_3,attachment_total_view_time_millis,attachment_view_completion,avg_screen_time_millis,avg_view_time_millis,impressions,ios_installs,quartile_1,quartile_2,quartile_3,screen_time_millis,spend,swipe_up_percent,swipes,total_installs,video_views,video_views_time_based,video_views_15s,view_completion,view_time_millis,conversion_purchases,conversion_purchases_value,conversion_save,conversion_start_checkout,conversion_add_cart,conversion_view_content,conversion_add_billing,conversion_sign_ups,conversion_searches,conversion_level_completes,conversion_app_opens,conversion_page_views,conversion_subscribe,conversion_ad_click,conversion_ad_view,conversion_complete_tutorial,conversion_invite,conversion_login,conversion_share,conversion_reserve,conversion_achievement_unlocked,conversion_add_to_wishlist,conversion_spend_credits,conversion_rate,conversion_start_trial,conversion_list_view,custom_event_1,custom_event_2,custom_event_3,custom_event_4,custom_event_5,attachment_frequency,attachment_uniques,frequency,uniques'

STREAMS = {
    # Reference: https://developers.snapchat.com/api/docs/#get-all-organizations
    'organizations': {
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['updated_at'],
        'path': 'me/organizations',
        'data_key_array': 'organizations',
        'data_key_record': 'organization',
        'paging': False,
        'params': {},
        'children': {
            # Reference: https://developers.snapchat.com/api/docs/?python#funding-sources
            'funding_sources': {
                'key_properties': ['id'],
                'replication_method': 'INCREMENTAL',
                'replication_keys': ['updated_at'],
                'path': 'organizations/{parent_id}/fundingsources',
                'data_key_array': 'fundingsources',
                'data_key_record': 'fundingsource',
                'paging': False,
                'parent': 'organization',
                'params': {}
            },
            # Reference: https://developers.snapchat.com/api/docs/?python#get-all-billing-centers
            'billing_centers': {
                'key_properties': ['id'],
                'replication_method': 'INCREMENTAL',
                'replication_keys': ['updated_at'],
                'path': 'organizations/{parent_id}/billingcenters',
                'data_key_array': 'billingcenters',
                'data_key_record': 'billingcenter',
                'paging': True,
                'parent': 'organization',
                'params': {}
            },
            # Reference: https://developers.snapchat.com/api/docs/?python#get-all-members-of-an-organization
            'members': {
                'key_properties': ['id'],
                'replication_method': 'INCREMENTAL',
                'replication_keys': ['updated_at'],
                'path': 'organizations/{parent_id}/members',
                'data_key_array': 'members',
                'data_key_record': 'member',
                'paging': False,
                'parent': 'organization',
                'params': {}
            },
            # Reference: https://developers.snapchat.com/api/docs/?python#get-all-roles-in-organization
            'roles': {
                'key_properties': ['id'],
                'replication_method': 'INCREMENTAL',
                'replication_keys': ['updated_at'],
                'path': 'organizations/{parent_id}/roles',
                'data_key_array': 'roles',
                'data_key_record': 'role',
                'paging': True,
                'parent': 'organization',
                'params': {}
            },
            # Reference: https://developers.snapchat.com/api/docs/#get-all-ad-accounts
            'ad_accounts': {
                'key_properties': ['id'],
                'replication_method': 'INCREMENTAL',
                'replication_keys': ['updated_at'],
                'path': 'organizations/{parent_id}/adaccounts',
                'data_key_array': 'adaccounts',
                'data_key_record': 'adaccount',
                'paging': False,
                'parent': 'organization',
                'params': {},
                'children': {
                    # Reference: https://developers.snapchat.com/api/docs/#get-ad-account-stats
                    'ad_account_stats_daily': {
                        'json_schema': 'schemas/shared/ad_account_stats.json',
                        'key_properties': ['id', 'start_time'],
                        'replication_method': 'INCREMENTAL', # query filtered
                        'replication_keys': ['end_time'],
                        'bookmark_query_field_from': 'start_time',
                        'bookmark_query_field_to': 'end_time',
                        'path': 'adaccounts/{parent_id}/stats',
                        'data_key_array': 'timeseries_stats',
                        'data_key_record': 'timeseries_stat',
                        'date_window_size': 30,
                        'paging': False,
                        'parent': 'ad_account',
                        'params': {
                            # 'test': 'true', # COMMENT OUT
                            'fields': 'spend',
                            'granularity': 'DAY',
                            'omit_empty': 'false',
                            'conversion_source_types': 'web,app,total',
                            'swipe_up_attribution_window': '{swipe_up_attribution_window}',
                            'view_attribution_window': '{view_attribution_window}'
                        }
                    },
                    # Reference: https://developers.snapchat.com/api/docs/#get-ad-account-stats
                    'ad_account_stats_hourly': {
                        'json_schema': 'schemas/shared/ad_account_stats.json',
                        'key_properties': ['id', 'start_time'],
                        'replication_method': 'INCREMENTAL', # query filtered
                        'replication_keys': ['end_time'],
                        'bookmark_query_field_from': 'start_time',
                        'bookmark_query_field_to': 'end_time',
                        'path': 'adaccounts/{parent_id}/stats',
                        'data_key_array': 'timeseries_stats',
                        'data_key_record': 'timeseries_stat',
                        'date_window_size': 7,
                        'paging': False,
                        'parent': 'ad_account',
                        'params': {
                            # 'test': 'true', # COMMENT OUT
                            'fields': 'spend',
                            'granularity': 'HOUR',
                            'omit_empty': 'false',
                            'conversion_source_types': 'web,app,total',
                            'swipe_up_attribution_window': '{swipe_up_attribution_window}',
                            'view_attribution_window': '{view_attribution_window}'
                        }
                    },
                    # Reference: https://developers.snapchat.com/api/docs/#get-all-audience-segments
                    'audience_segments': {
                        'key_properties': ['id'],
                        'replication_method': 'INCREMENTAL',
                        'replication_keys': ['updated_at'],
                        'path': 'adaccounts/{parent_id}/segments',
                        'data_key_array': 'segments',
                        'data_key_record': 'segment',
                        'paging': True,
                        'parent': 'ad_account',
                        'params': {}
                    },
                    # Reference: https://developers.snapchat.com/api/docs/#get-the-pixel-associated-with-an-ad-account
                    'pixels': {
                        'key_properties': ['id'],
                        'replication_method': 'INCREMENTAL',
                        'replication_keys': ['updated_at'],
                        'path': 'adaccounts/{parent_id}/pixels',
                        'data_key_array': 'pixels',
                        'data_key_record': 'pixel',
                        'paging': True,
                        'parent': 'ad_account',
                        'params': {},
                        'children': {
                            # Reference: https://developers.snapchat.com/api/docs/#get-pixel-domains
                            'pixel_domain_stats': {
                                'key_properties': ['id'],
                                'replication_method': 'FULL_TABLE',
                                'path': 'pixels/{parent_id}/domains/stats',
                                'data_key_array': 'timeseries_stats',
                                'data_key_record': 'timeseries_stat',
                                'paging': False,
                                'parent': 'pixel',
                                'params': {}
                            }
                        }
                    },
                    # Reference: https://developers.snapchat.com/api/docs/#get-all-media
                    'media': {
                        'key_properties': ['id'],
                        'replication_method': 'INCREMENTAL',
                        'replication_keys': ['updated_at'],
                        'path': 'adaccounts/{parent_id}/media',
                        'data_key_array': 'media',
                        'data_key_record': 'media',
                        'paging': True,
                        'parent': 'ad_account',
                        'params': {}
                    },
                    # Reference: https://developers.snapchat.com/api/docs/#get-all-creatives
                    'creatives': {
                        'key_properties': ['id'],
                        'replication_method': 'INCREMENTAL',
                        'replication_keys': ['updated_at'],
                        'path': 'adaccounts/{parent_id}/creatives',
                        'data_key_array': 'creatives',
                        'data_key_record': 'creative',
                        'paging': True,
                        'parent': 'ad_account',
                        'params': {}
                    },
                    # Reference: https://developers.snapchat.com/api/docs/#swipe-to-call-text-phone-number-verification
                    'phone_numbers': {
                        'key_properties': ['id'],
                        'replication_method': 'INCREMENTAL',
                        'replication_keys': ['updated_at'],
                        'path': 'adaccounts/{parent_id}/phone_numbers',
                        'data_key_array': 'phone_numbers',
                        'data_key_record': 'phone_number',
                        'paging': True,
                        'parent': 'ad_account',
                        'params': {}
                    },

                    # Reference: https://developers.snapchat.com/api/docs/#get-all-campaigns
                    'campaigns': {
                        'key_properties': ['id'],
                        'replication_method': 'INCREMENTAL',
                        'replication_keys': ['updated_at'],
                        'path': 'adaccounts/{parent_id}/campaigns',
                        'data_key_array': 'campaigns',
                        'data_key_record': 'campaign',
                        'paging': True,
                        'parent': 'ad_account',
                        'params': {},
                        'children': {
                            # Reference: https://developers.snapchat.com/api/docs/#get-campaign-stats
                            'campaign_stats_daily': {
                                'json_schema': 'schemas/shared/stats.json',
                                'key_properties': ['id', 'start_time'],
                                'replication_method': 'INCREMENTAL', # query filtered
                                'replication_keys': ['end_time'],
                                'bookmark_query_field_from': 'start_time',
                                'bookmark_query_field_to': 'end_time',
                                'path': 'campaigns/{parent_id}/stats',
                                'data_key_array': 'timeseries_stats',
                                'data_key_record': 'timeseries_stat',
                                'date_window_size': 30,
                                'paging': False,
                                'parent': 'campaign',
                                'params': {
                                    # 'test': 'true', # COMMENT OUT
                                    'fields': ALL_STATS_FIELDS,
                                    'granularity': 'DAY',
                                    'omit_empty': 'false',
                                    'conversion_source_types': 'web,app,total',
                                    'swipe_up_attribution_window': '{swipe_up_attribution_window}',
                                    'view_attribution_window': '{view_attribution_window}'
                                }
                            },
                            # Reference: https://developers.snapchat.com/api/docs/#get-campaign-stats
                            'campaign_stats_hourly': {
                                'json_schema': 'schemas/shared/stats.json',
                                'key_properties': ['id', 'start_time'],
                                'replication_method': 'INCREMENTAL', # query filtered
                                'replication_keys': ['end_time'],
                                'bookmark_query_field_from': 'start_time',
                                'bookmark_query_field_to': 'end_time',
                                'path': 'campaigns/{parent_id}/stats',
                                'data_key_array': 'timeseries_stats',
                                'data_key_record': 'timeseries_stat',
                                'date_window_size': 7,
                                'paging': False,
                                'parent': 'campaign',
                                'params': {
                                    # 'test': 'true', # COMMENT OUT
                                    'fields': ALL_STATS_FIELDS,
                                    'granularity': 'HOUR',
                                    'omit_empty': 'false',
                                    'conversion_source_types': 'web,app,total',
                                    'swipe_up_attribution_window': '{swipe_up_attribution_window}',
                                    'view_attribution_window': '{view_attribution_window}'
                                }
                            }
                        }
                    },
                    # Reference: https://developers.snapchat.com/api/docs/#get-all-ad-squads-under-an-ad-account
                    'ad_squads': {
                        'key_properties': ['id'],
                        'replication_method': 'INCREMENTAL',
                        'replication_keys': ['updated_at'],
                        'path': 'adaccounts/{parent_id}/adsquads',
                        'data_key_array': 'adsquads',
                        'data_key_record': 'adsquad',
                        'paging': True,
                        'parent': 'ad_account',
                        'params': {},
                        'children': {
                            # Reference: https://developers.snapchat.com/api/docs/#get-ad-squad-stats
                            'ad_squad_stats_daily': {
                                'json_schema': 'schemas/shared/stats.json',
                                'key_properties': ['id', 'start_time'],
                                'replication_method': 'INCREMENTAL', # query filtered
                                'replication_keys': ['end_time'],
                                'bookmark_query_field_from': 'start_time',
                                'bookmark_query_field_to': 'end_time',
                                'path': 'adsquads/{parent_id}/stats',
                                'data_key_array': 'timeseries_stats',
                                'data_key_record': 'timeseries_stat',
                                'date_window_size': 30,
                                'paging': False,
                                'parent': 'ad_squad',
                                'params': {
                                    # 'test': 'true', # COMMENT OUT
                                    'fields': ALL_STATS_FIELDS,
                                    'granularity': 'DAY',
                                    'omit_empty': 'false',
                                    'conversion_source_types': 'web,app,total',
                                    'swipe_up_attribution_window': '{swipe_up_attribution_window}',
                                    'view_attribution_window': '{view_attribution_window}'
                                }
                            },
                            # Reference: https://developers.snapchat.com/api/docs/#get-ad-squad-stats
                            'ad_squad_stats_hourly': {
                                'json_schema': 'schemas/shared/stats.json',
                                'key_properties': ['id', 'start_time'],
                                'replication_method': 'INCREMENTAL', # query filtered
                                'replication_keys': ['end_time'],
                                'bookmark_query_field_from': 'start_time',
                                'bookmark_query_field_to': 'end_time',
                                'path': 'adsquads/{parent_id}/stats',
                                'data_key_array': 'timeseries_stats',
                                'data_key_record': 'timeseries_stat',
                                'date_window_size': 7,
                                'paging': False,
                                'parent': 'ad_squad',
                                'params': {
                                    # 'test': 'true', # COMMENT OUT
                                    'fields': ALL_STATS_FIELDS,
                                    'granularity': 'HOUR',
                                    'omit_empty': 'false',
                                    'conversion_source_types': 'web,app,total',
                                    'swipe_up_attribution_window': '{swipe_up_attribution_window}',
                                    'view_attribution_window': '{view_attribution_window}'
                                }
                            }
                        }
                    },
                    # Reference: https://developers.snapchat.com/api/docs/#get-all-ads-under-an-ad-account
                    'ads': {
                        'key_properties': ['id'],
                        'replication_method': 'INCREMENTAL',
                        'replication_keys': ['updated_at'],
                        'path': 'adaccounts/{parent_id}/ads',
                        'data_key_array': 'ads',
                        'data_key_record': 'ad',
                        'paging': True,
                        'parent': 'ad_account',
                        'params': {},
                        'children': {
                            # Reference: https://developers.snapchat.com/api/docs/#get-ad-stats
                            'ad_stats_daily': {
                                'json_schema': 'schemas/shared/stats.json',
                                'key_properties': ['id', 'start_time'],
                                'replication_method': 'INCREMENTAL', # query filtered
                                'replication_keys': ['end_time'],
                                'bookmark_query_field_from': 'start_time',
                                'bookmark_query_field_to': 'end_time',
                                'path': 'ads/{parent_id}/stats',
                                'data_key_array': 'timeseries_stats',
                                'data_key_record': 'timeseries_stat',
                                'date_window_size': 30,
                                'paging': False,
                                'parent': 'ad',
                                'params': {
                                    # 'test': 'true', # COMMENT OUT
                                    'fields': ALL_STATS_FIELDS,
                                    'granularity': 'DAY',
                                    'omit_empty': 'false',
                                    'conversion_source_types': 'web,app,total',
                                    'swipe_up_attribution_window': '{swipe_up_attribution_window}',
                                    'view_attribution_window': '{view_attribution_window}'
                                }
                            },
                            # Reference: https://developers.snapchat.com/api/docs/#get-ad-stats
                            'ad_stats_hourly': {
                                'json_schema': 'schemas/shared/stats.json',
                                'key_properties': ['id', 'start_time'],
                                'replication_method': 'INCREMENTAL', # query filtered
                                'replication_keys': ['end_time'],
                                'bookmark_query_field_from': 'start_time',
                                'bookmark_query_field_to': 'end_time',
                                'path': 'ads/{parent_id}/stats',
                                'data_key_array': 'timeseries_stats',
                                'data_key_record': 'timeseries_stat',
                                'date_window_size': 7,
                                'paging': False,
                                'parent': 'ad',
                                'params': {
                                    # 'test': 'true', # COMMENT OUT
                                    'fields': ALL_STATS_FIELDS,
                                    'granularity': 'HOUR',
                                    'omit_empty': 'false',
                                    'conversion_source_types': 'web,app,total',
                                    'swipe_up_attribution_window': '{swipe_up_attribution_window}',
                                    'view_attribution_window': '{view_attribution_window}'
                                }
                            }
                        }
                    },
                }
            },
            # Reference: https://developers.snapchat.com/api/docs/#get-all-catalogs
            'product_catalogs': {
                'key_properties': ['id'],
                'replication_method': 'INCREMENTAL',
                'replication_keys': ['updated_at'],
                'path': 'organizations/{parent_id}/catalogs',
                'data_key_array': 'catalogs',
                'data_key_record': 'catalog',
                'paging': True,
                'parent': 'organization',
                'params': {},
                'children': {
                    # Reference: https://developers.snapchat.com/api/docs/#get-all-product-sets
                    'product_sets': {
                        'key_properties': ['id'],
                        'replication_method': 'FULL_TABLE',
                        'path': 'catalogs/{parent_id}/product_sets',
                        'data_key_array': 'product_sets',
                        'data_key_record': 'product_set',
                        'paging': True,
                        'parent': 'product_catalog',
                        'params': {}
                    }
                }
            },
        }
    },

    # DEMO - AGE GROUP
    # Reference: https://developers.snapchat.com/api/docs/#demographics-age-groups
    'targeting_age_groups': {
        'json_schema': 'schemas/shared/targeting.json',
        'targeting_group': 'demographics',
        'targeting_type': 'age_group',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'path': 'targeting/{targeting_group}/{targeting_type}',
        'data_key_array': 'targeting_dimensions',
        'data_key_record': '{targeting_type}',
        'paging': True,
        'params': {}
    },
    # DEMO - GENDER
    # Reference: https://developers.snapchat.com/api/docs/#demographics-gender
    'targeting_genders': {
        'json_schema': 'schemas/shared/targeting.json',
        'targeting_group': 'demographics',
        'targeting_type': 'gender',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'path': 'targeting/{targeting_group}/{targeting_type}',
        'data_key_array': 'targeting_dimensions',
        'data_key_record': '{targeting_type}',
        'paging': True,
        'params': {}
    },
    # DEMO - LANGUAGES
    # Reference: https://developers.snapchat.com/api/docs/#demographics-language
    'targeting_languages': {
        'json_schema': 'schemas/shared/targeting.json',
        'targeting_group': 'demographics',
        'targeting_type': 'languages',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'path': 'targeting/{targeting_group}/{targeting_type}',
        'data_key_array': 'targeting_dimensions',
        'data_key_record': '{targeting_type}',
        'paging': True,
        'params': {}
    },
    # DEMO - ADVANCED DEMOGRAPHICS
    # Reference: https://developers.snapchat.com/api/docs/#demographics-advanced-demographics
    'targeting_advanced_demographics': {
        'json_schema': 'schemas/shared/targeting.json',
        'targeting_group': 'demographics',
        'targeting_type': 'advanced_demographics',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'path': 'targeting/{targeting_group}/{targeting_type}',
        'data_key_array': 'targeting_dimensions',
        'data_key_record': '{targeting_type}',
        'paging': True,
        'params': {}
    },

    # DEVICE - CONNECTION TYPE
    # Reference: https://developers.snapchat.com/api/docs/#device-connection-type
    'targeting_connection_types': {
        'json_schema': 'schemas/shared/targeting.json',
        'targeting_group': 'device',
        'targeting_type': 'connection_type',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'path': 'targeting/{targeting_group}/{targeting_type}',
        'data_key_array': 'targeting_dimensions',
        'data_key_record': '{targeting_type}',
        'paging': True,
        'params': {}
    },
    # DEVICE - OS TYPE
    # Reference: https://developers.snapchat.com/api/docs/#device-os-type
    'targeting_os_types': {
        'json_schema': 'schemas/shared/targeting.json',
        'targeting_group': 'device',
        'targeting_type': 'os_type',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'path': 'targeting/{targeting_group}/{targeting_type}',
        'data_key_array': 'targeting_dimensions',
        'data_key_record': '{targeting_type}',
        'paging': True,
        'params': {}
    },
    # DEVICE - OS VERSION - iOS
    # Reference: https://developers.snapchat.com/api/docs/#device-os-version
    'targeting_ios_versions': {
        'json_schema': 'schemas/shared/targeting.json',
        'targeting_group': 'device',
        'targeting_type': 'os_version',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'path': 'targeting/device/iOS/os_version',
        'data_key_array': 'targeting_dimensions',
        'data_key_record': '{targeting_type}',
        'paging': True,
        'params': {}
    },
    # DEVICE - OS VERSION - ANDROID
    # Reference: https://developers.snapchat.com/api/docs/#device-os-version
    'targeting_android_versions': {
        'json_schema': 'schemas/shared/targeting.json',
        'targeting_group': 'device',
        'targeting_type': 'os_version',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'path': 'targeting/device/ANDROID/os_version',
        'data_key_array': 'targeting_dimensions',
        'data_key_record': '{targeting_type}',
        'paging': True,
        'params': {}
    },
    # DEVICE - CARRIER
    # Reference: https://developers.snapchat.com/api/docs/#device-carrier
    'targeting_carriers': {
        'json_schema': 'schemas/shared/targeting.json',
        'targeting_group': 'device',
        'targeting_type': 'carrier',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'path': 'targeting/{targeting_group}/{targeting_type}',
        'data_key_array': 'targeting_dimensions',
        'data_key_record': '{targeting_type}',
        'paging': True,
        'params': {}
    },
    # DEVICE - MARKETING NAME
    # Reference: https://developers.snapchat.com/api/docs/#device-make
    'targeting_device_makes': {
        'json_schema': 'schemas/shared/targeting.json',
        'targeting_group': 'device',
        'targeting_type': 'marketing_name',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'path': 'targeting/{targeting_group}/{targeting_type}',
        'data_key_array': 'targeting_dimensions',
        'data_key_record': '{targeting_type}',
        'paging': True,
        'params': {}
    },

    # GEO - COUNTRY
    # Reference: https://developers.snapchat.com/api/docs/#geolocation-country
    'targeting_countries': {
        'json_schema': 'schemas/shared/targeting_geo.json',
        'targeting_group': 'geo',
        'targeting_type': 'country',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'path': 'targeting/{targeting_group}/{targeting_type}',
        'data_key_array': 'targeting_dimensions',
        'data_key_record': '{targeting_type}',
        'paging': True,
        'params': {}
    },
    # GEO - REGION
    # Reference: https://developers.snapchat.com/api/docs/#geolocation-region-state
    'targeting_regions': {
        'json_schema': 'schemas/shared/targeting_geo.json',
        'targeting_group': 'geo',
        'targeting_type': 'region',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'path': 'targeting/geo/{country_code}/region',
        'targeting_country_ind': True,
        'data_key_array': 'targeting_dimensions',
        'data_key_record': '{targeting_type}',
        'api_method': 'GET',
        'paging': True,
        'params': {}
    },
    # GEO - METRO
    # Reference: https://developers.snapchat.com/api/docs/#geolocation-metro-dma
    'targeting_metros': {
        'json_schema': 'schemas/shared/targeting_geo.json',
        'targeting_group': 'geo',
        'targeting_type': 'metro',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'path': 'targeting/geo/{country_code}/metro',
        'targeting_country_ind': True,
        'data_key_array': 'targeting_dimensions',
        'data_key_record': '{targeting_type}',
        'api_method': 'GET',
        'paging': True,
        'params': {}
    },
    # GEO - POSTAL CODE
    # Reference: https://developers.snapchat.com/api/docs/#zipcode
    'targeting_postal_codes': {
        'json_schema': 'schemas/shared/targeting_geo.json',
        'targeting_group': 'geo',
        'targeting_type': 'postal_code',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'path': 'targeting/geo/{country_code}/postal_code',
        'targeting_country_ind': True,
        'data_key_array': 'targeting_dimensions',
        'data_key_record': '{targeting_type}',
        'api_method': 'GET',
        'paging': True,
        'params': {}
    },

    # INTERESTS - SCLS (Snap Lifestyle Categories)
    # Reference: https://developers.snapchat.com/api/docs/#interests-snap-lifestyle-categories
    'targeting_interests_scls': {
        'json_schema': 'schemas/shared/targeting.json',
        'targeting_group': 'interests',
        'targeting_type': 'scls',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'path': 'targeting/{targeting_group}/{targeting_type}',
        'data_key_array': 'targeting_dimensions',
        'data_key_record': '{targeting_type}',
        'api_method': 'GET',
        'paging': True,
        'params': {}
    },
    # INTERESTS - DLXS (Oracle Datalogix)
    # Reference: https://developers.snapchat.com/api/docs/#get-oracle-datalogix-dlxs-interest-targeting-options
    'targeting_interests_dlxs': {
        'json_schema': 'schemas/shared/targeting.json',
        'targeting_group': 'interests',
        'targeting_type': 'dlxs',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'path': 'targeting/{targeting_group}/{targeting_type}',
        'data_key_array': 'targeting_dimensions',
        'data_key_record': '{targeting_type}',
        'api_method': 'GET',
        'paging': True,
        'params': {}
    },
    # INTERESTS - DLXC (Oracle Datalogix)
    # Reference: https://developers.snapchat.com/api/docs/#get-oracle-datalogix-dlxc-interest-targeting-options
    'targeting_interests_dlxc': {
        'json_schema': 'schemas/shared/targeting.json',
        'targeting_group': 'interests',
        'targeting_type': 'dlxc',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'path': 'targeting/{targeting_group}/{targeting_type}',
        'data_key_array': 'targeting_dimensions',
        'data_key_record': '{targeting_type}',
        'api_method': 'GET',
        'paging': True,
        'params': {}
    },
    # INTERESTS - DLXP (Oracle Datalogix)
    # Reference: https://developers.snapchat.com/api/docs/#get-oracle-datalogix-dlxp-interest-targeting-options
    'targeting_interests_dlxp': {
        'json_schema': 'schemas/shared/targeting.json',
        'targeting_group': 'interests',
        'targeting_type': 'dlxp',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'path': 'targeting/{targeting_group}/{targeting_type}',
        'data_key_array': 'targeting_dimensions',
        'data_key_record': '{targeting_type}',
        'api_method': 'GET',
        'paging': True,
        'params': {}
    },
    # INTERESTS - NLN (Nielsen)
    # Reference: https://developers.snapchat.com/api/docs/#get-nielsen-interest-targeting-options
    'targeting_interests_nln': {
        'json_schema': 'schemas/shared/targeting.json',
        'targeting_group': 'interests',
        'targeting_type': 'nln',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'path': 'targeting/{targeting_group}/{targeting_type}',
        'data_key_array': 'targeting_dimensions',
        'data_key_record': '{targeting_type}',
        'api_method': 'GET',
        'paging': True,
        'params': {}
    },
    # INTERESTS - PLC (Placed Visitation Segments)
    # Reference: https://developers.snapchat.com/api/docs/#interests-placed-visitation-segments
    'targeting_interests_plc': {
        'json_schema': 'schemas/shared/targeting.json',
        'targeting_group': 'interests',
        'targeting_type': 'plc',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'path': 'targeting/{targeting_group}/{targeting_type}',
        'data_key_array': 'targeting_dimensions',
        'data_key_record': '{targeting_type}',
        'api_method': 'GET',
        'paging': True,
        'params': {}
    },

    # LOCATION - Categories LOI
    # Reference: https://developers.snapchat.com/api/docs/#location
    'targeting_location_categories': {
        'json_schema': 'schemas/shared/targeting.json',
        'targeting_group': 'location',
        'targeting_type': 'categories_loi',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'path': 'targeting/{targeting_group}/{targeting_type}',
        'data_key_array': 'targeting_dimensions',
        'data_key_record': '{targeting_type}',
        'api_method': 'GET',
        'paging': True,
        'params': {}
    }
}


# De-nest children nodes for Discovery mode
def flatten_streams():
    flat_streams = {}
    # Loop through parents
    for stream_name, endpoint_config in STREAMS.items():
        flat_streams[stream_name] = endpoint_config
        # Loop through children
        children = endpoint_config.get('children')
        if children:
            for child_stream_name, child_endpoint_config in children.items():
                flat_streams[child_stream_name] = child_endpoint_config
                flat_streams[child_stream_name]['parent_stream'] = stream_name
                # Loop through grandchildren
                grandchildren = child_endpoint_config.get('children')
                if grandchildren:
                    for grandchild_stream_name, grandchild_endpoint_config in grandchildren.items():
                        flat_streams[grandchild_stream_name] = grandchild_endpoint_config
                        flat_streams[grandchild_stream_name]['parent_stream'] = child_stream_name
                        flat_streams[grandchild_stream_name]['grandparent_stream'] = stream_name
                        # Loop through great_grandchildren
                        great_grandchildren = grandchild_endpoint_config.get('children')
                        if great_grandchildren:
                            for great_grandchild_stream_name, great_grandchild_endpoint_config in great_grandchildren.items():
                                flat_streams[great_grandchild_stream_name] = great_grandchild_endpoint_config
                                flat_streams[great_grandchild_stream_name]['parent_stream'] = grandchild_stream_name
                                flat_streams[grandchild_stream_name]['grandparent_stream'] = child_stream_name
                                flat_streams[grandchild_stream_name]['great_grandparent_stream'] = stream_name

    return flat_streams
