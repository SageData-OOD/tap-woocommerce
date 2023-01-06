#!/usr/bin/env python3
import os
import json
import attr
import pendulum

import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from singer.transform import transform

from dateutil import parser
from tap_woocommerce.api_wrapper import ApiWrapper

REQUIRED_CONFIG_KEYS = ["url", "consumer_key", "consumer_secret", "start_date"]
LOGGER = singer.get_logger()
CONFIG = {
    "url": None,
    "consumer_key": None,
    "consumer_secret": None,
    "start_date": None
}


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def get_bookmark(stream_id):
    bookmark = {
        "orders": "date_created",
        "reports": "date_end"
    }
    return bookmark.get(stream_id)


def create_metadata_for_streams(stream_id, schema, key_properties):
    replication_key = get_bookmark(stream_id)
    mdata = [{"breadcrumb": [], "metadata": {
        "inclusion": "available", "forced-replication-method": "INCREMENTAL",
        "valid-replication-keys": [replication_key]}}]

    if replication_key is None:
        mdata[0]["metadata"]["forced-replication-method"] = "FULL_TABLE"
        mdata[0]["metadata"].pop("valid-replication-keys")

    for key in schema.properties:
        # hence, when property is object, we will only consider properties of that object without taking object itself.
        if "object" in schema.properties.get(key).type:
            inclusion = "available"
            mdata.extend(
                [{"breadcrumb": ["properties", key, "properties", prop], "metadata": {"inclusion": inclusion}} for prop
                 in schema.properties.get(key).properties])
        else:
            inclusion = "automatic" if key in key_properties + [replication_key] else "available"
            mdata.append({"breadcrumb": ["properties", key], "metadata": {"inclusion": inclusion}})

    return mdata


def giveup(exc):
    return exc.response is not None \
        and 400 <= exc.response.status_code < 500 \
        and exc.response.status_code != 429


def get_end_date(start_date):
    end_date = pendulum.parse(start_date).add(months=1)
    if end_date > pendulum.yesterday():
        end_date = pendulum.yesterday()
    return end_date.to_date_string()


def sync_orders(woocommerce_client, config, state, stream):
    bookmark_column = get_bookmark(stream.tap_stream_id)
    mdata = metadata.to_map(stream.metadata)
    schema = stream.schema.to_dict()

    singer.write_schema(
        stream_name=stream.tap_stream_id,
        schema=schema,
        key_properties=stream.key_properties,
    )

    start_date = singer.get_bookmark(state, stream.tap_stream_id, bookmark_column).split(" ")[0] \
        if state.get("bookmarks", {}).get(stream.tap_stream_id) else config["start_date"]

    LOGGER.info("Only syncing orders updated since " + start_date)
    last_update = start_date
    page_number = 1
    with singer.metrics.record_counter(stream.tap_stream_id) as counter:
        params = {
            "after": last_update,
            "orderby": "date",
            "order": "asc",
            "per_page": "100",
            "page": page_number
        }
        while True:
            orders = woocommerce_client.orders(params)
            # with open("sync_orders.json", "w") as fout:
            #     fout.write(json.dumps(orders, indent=4))

            for order in orders:
                counter.increment()
                # row = transform(order, schema, metadata=mdata)
                row = order
                if parser.parse(row[bookmark_column]).replace(tzinfo=None) > parser.parse(last_update).replace(
                        tzinfo=None):
                    last_update = row[bookmark_column]
                singer.write_record("orders", row)
            if len(orders) < 100:
                break
            page_number += 1
    state = singer.write_bookmark(state, 'orders', bookmark_column, last_update)
    singer.write_state(state)
    LOGGER.info("Completed Orders Sync")


def sync_reports(woocommerce_client, config, state, stream):
    bookmark_column = get_bookmark(stream.tap_stream_id)
    mdata = metadata.to_map(stream.metadata)
    schema = stream.schema.to_dict()

    singer.write_schema(
        stream_name=stream.tap_stream_id,
        schema=schema,
        key_properties=stream.key_properties,
    )

    start_date = singer.get_bookmark(state, stream.tap_stream_id, bookmark_column).split(" ")[0] \
        if state.get("bookmarks", {}).get(stream.tap_stream_id) else config["start_date"]

    start_date = start_date[:10]
    LOGGER.info("Only syncing reports updated since " + start_date)
    last_update = start_date
    with singer.metrics.record_counter(stream.tap_stream_id) as counter:
        while True:
            report_response = woocommerce_client.reports(last_update, last_update)
            for resp in report_response:
                # transformed_data = transform(resp, schema, metadata=mdata)
                transformed_data = resp
                singer.write_records(stream.tap_stream_id, [transformed_data])
                counter.increment()
            if pendulum.parse(last_update).to_date_string() == pendulum.yesterday().to_date_string():
                break
            last_update = pendulum.parse(last_update).add(days=1).to_date_string()
    state = singer.write_bookmark(state, 'reports', 'date_end', last_update)
    singer.write_state(state)


@attr.s
class Stream(object):
    tap_stream_id = attr.ib()
    sync = attr.ib()


def get_sync_function(stram_id):
    sync_stream = {
        "orders": sync_orders,
        "reports": sync_reports
    }
    return sync_stream[stram_id]


def get_selected_streams(remaining_streams, annotated_schema):
    selected_streams = []

    for stream in remaining_streams:
        tap_stream_id = stream.tap_stream_id
        for stream_idx, annotated_stream in enumerate(annotated_schema.streams):
            if tap_stream_id == annotated_stream.tap_stream_id:
                schema = annotated_stream.schema
                if (hasattr(schema, "selected")) and (schema.selected is True):
                    selected_streams.append(stream)

    return selected_streams


def sync(config, state, catalog):
    woocommerce_client = ApiWrapper(config["url"], config["consumer_key"], config["consumer_secret"])

    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)
        stream_sync = get_sync_function(stream.tap_stream_id)
        stream_sync(woocommerce_client, config, state, stream)


def get_abs_path(path):
    """Returns the absolute path"""
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def get_key_properties(stream_id):
    key_properties = {
        "orders": ["order_id"],
        "reports": ["date_start"]
    }
    return key_properties.get(stream_id, [])


def do_discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_metadata = create_metadata_for_streams(stream_id, schema, get_key_properties(stream_id))
        key_properties = get_key_properties(stream_id)
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata
            )
        )
    return Catalog(streams)


@utils.handle_top_exception(LOGGER)
def main():
    """Entry point"""
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    CONFIG.update(args.config)
    STATE = {}

    if args.state:
        STATE.update(args.state)
    if args.discover:
        catalog = do_discover()
        catalog.dump()
    elif args.catalog:
        sync(CONFIG, STATE, args.catalog)
    else:
        LOGGER.info("No Streams were selected")


if __name__ == "__main__":
    main()
