#!/usr/bin/env python3

import sys
import json
import argparse
import singer
from singer import metadata, utils
from tap_snapchat_ads.client import SnapchatClient
from tap_snapchat_ads.discover import discover
from tap_snapchat_ads.sync import sync as _sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'client_id',
    'client_secret',
    'refresh_token',
    'start_date',
    'user_agent'
]

def do_discover():

    LOGGER.info('Starting discover')
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')


@singer.utils.handle_top_exception(LOGGER)
def main():

    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    with SnapchatClient(parsed_args.config['client_id'],
                        parsed_args.config['client_secret'],
                        parsed_args.config['refresh_token'],
                        parsed_args.config.get('request_timeout'),
                        parsed_args.config['user_agent']) as client:

        state = {}
        if parsed_args.state:
            state = parsed_args.state

        config = {}
        if parsed_args.config:
            config = parsed_args.config

        if parsed_args.discover:
            do_discover()
        elif parsed_args.catalog:
            _sync(client=client,
                 config=config,
                 catalog=parsed_args.catalog,
                 state=state)

if __name__ == '__main__':
    main()
