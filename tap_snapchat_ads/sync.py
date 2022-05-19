import singer
from tap_snapchat_ads.streams import STREAMS, ROOT_STREAMS, update_currently_syncing

LOGGER = singer.get_logger()

# Function for sync mode
def sync(client, config, catalog, state):
    # Get selected_streams from catalog, based on state last_stream
    #   last_stream = Previous currently synced stream, if the load was interrupted
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info('last/currently syncing stream: {}'.format(last_stream))
    selected_streams = []
    for stream in catalog.get_selected_streams(state):
        selected_streams.append(stream.stream)
    LOGGER.info('selected_streams: {}'.format(selected_streams))
    if not selected_streams or selected_streams == []:
        return

    # Get the streams to sync (based on dependencies)
    sync_streams = []
    # Loop thru all streams
    for stream_name, stream_class in STREAMS.items():
        # only sync streams from last stream ie. 'currently_syncing' stream
        if last_stream and last_stream != stream_name:
            continue
        last_stream = None
        # If stream has a parent_stream, then it is a child stream
        parent_stream = stream_class.parent_stream
        grandparent_stream = stream_class.grandparent_stream
        great_grandparent_stream = stream_class.great_grandparent_stream

        if stream_name in selected_streams:
            LOGGER.info('stream: {}, parent: {}, grandparent: {}, great_grandparent: {}'.format(
                stream_name, parent_stream, grandparent_stream, great_grandparent_stream))
            if stream_name not in sync_streams:
                sync_streams.append(stream_name)
            if parent_stream and parent_stream not in sync_streams:
                sync_streams.append(parent_stream)
            if grandparent_stream and grandparent_stream not in sync_streams:
                sync_streams.append(grandparent_stream)
            if great_grandparent_stream and great_grandparent_stream not in sync_streams:
                sync_streams.append(great_grandparent_stream)
    LOGGER.info('Sync Streams: {}'.format(sync_streams))

    # Loop through selected_streams
    # Loop through endpoints in selected_streams
    for stream_name, stream_class in ROOT_STREAMS.items():
        if stream_name in sync_streams:
            stream_obj = stream_class()
            LOGGER.info('START Syncing: {}'.format(stream_name))
            stream_obj.write_schema(catalog, stream_name, sync_streams, selected_streams)
            update_currently_syncing(state, stream_name)

            total_records = stream_obj.sync_endpoint(
                client=client,
                config=config,
                catalog=catalog,
                state=state,
                stream_name=stream_name,
                stream_class=stream_class,
                sync_streams=sync_streams,
                selected_streams=selected_streams)

            update_currently_syncing(state, None)
            LOGGER.info('FINISHED Syncing: {}, total_records: {}'.format(
                stream_name,
                total_records))

    # remove currently_syncing at the end of the sync this will help in
    # edge case scenario by handling infinite loop of empty state file
    update_currently_syncing(state, None)
