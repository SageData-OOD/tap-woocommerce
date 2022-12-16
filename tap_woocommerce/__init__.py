#!/usr/bin/env python3
import itertools
import os
import sys
import json
import attr
import singer
import singer.metrics as metrics
from singer.transform import transform
from singer import utils
from dateutil import parser
import pendulum
from tap_woocommerce.api_wrapper import ApiWrapper

REQUIRED_CONFIG_KEYS = ["url", "consumer_key", "consumer_secret", "start_date"]
REQUEST_USER_AGENT = 'VELOCITY'
LOGGER = singer.get_logger()
CONFIG = {
    "url": None,
    "consumer_key": None,
    "consumer_secret": None,
    "start_date": None
}
_api_wrapper = None


def get_start(STATE, tap_stream_id, bookmark_key):
    current_bookmark = singer.get_bookmark(STATE, tap_stream_id, bookmark_key)
    if current_bookmark is None:
        date = pendulum.parse(CONFIG["start_date"])
    else:
        date = pendulum.parse(current_bookmark)
    return date.strftime('%Y-%m-%dT%H:%M:%SZ')


def get_start_for_report(STATE, tap_stream_id, bookmark_key):
    current_bookmark = singer.get_bookmark(STATE, tap_stream_id, bookmark_key)
    if current_bookmark is None:
        return pendulum.parse(CONFIG["start_date"]).to_date_string()
    return pendulum.parse(current_bookmark).to_date_string()


def load_schema(entity):
    '''Returns the schema for the specified source'''
    schema = utils.load_json(get_abs_path("schemas/{}.json".format(entity)))

    return schema


def filter_items(item):
    filtered = {
        "id": int(item["id"]),
        "name": str(item["name"]),
        "product_id": int(item["product_id"]),
        "variation_id": int(item["variation_id"]),
        "quantity": int(item["quantity"]),
        "subtotal": float(item["subtotal"]),
        "subtotal_tax": float(item["subtotal_tax"]),
        "total": float(item["total"]),
        "sku": str(item["sku"]),
        "price": float(item["price"])
    }
    return filtered


def filter_coupons(coupon):
    filtered = {
        "id": int(coupon["id"]),
        "code": str(coupon["code"]),
        "discount": float(coupon["discount"]),
        "discount_tax": float(coupon["discount_tax"])
    }
    return filtered


def filter_shipping(ship):
    filtered = {
        "id": int(ship["id"]),
        "method_title": str(ship["method_title"]),
        "method_id": str(ship["method_id"]),
        "total": float(ship["total"])
    }
    return filtered


def filter_report_totals(totals, date):
    return {**totals, "date": date}


def get_last_date(totals, start_date):
    date = start_date
    for i in totals:
        date = (
            pendulum.parse(date) if pendulum.parse(date) > pendulum.parse(i) else pendulum.parse(i)).to_date_string()
    return date


def filter_report(report, start_date):
    if "totals" in report and len(report["totals"]) > 0:
        totals = [filter_report_totals(report["totals"][date], date) for date in report["totals"]]

    try:
        filtered = {**report, "totals": totals, "date_start": start_date, "date_end": start_date,
                    "store_url": CONFIG["url"]}
    except:
        filtered = {}

    return filtered


def filter_order(order):
    tzinfo = parser.parse(CONFIG["start_date"]).tzinfo
    if "line_items" in order and len(order["line_items"]) > 0:
        line_items = [filter_items(item) for item in order["line_items"]]
    else:
        line_items = None
    if "coupon_lines" in order and len(order["coupon_lines"]) > 0:
        coupon_lines = [filter_coupons(coupon) for coupon in order["coupon_lines"]]
    else:
        coupon_lines = None
    if "shipping_lines" in order and len(order["shipping_lines"]) > 0:
        shipping_lines = [filter_shipping(ship) for ship in order["shipping_lines"]]
    else:
        shipping_lines = None
    if "customer_id" in order and order["customer_id"] and order["customer_id"] != 0:
        params = {
            "customer": order["customer_id"]
        }
        customer = _api_wrapper.orders(params)
        new_customer = False if len(customer) > 1 else True
    else:
        new_customer = None
    try:
        filtered = {
            "number": int(order["number"]),
            "store_url": CONFIG["url"],
            "created_via": str(order["created_via"]),
            "currency": str(order["currency"]),
            "order_id": int(order["id"]),
            "order_key": str(order["order_key"]),
            "total_tax": float(order["total_tax"]),
            "discount_tax": float(order["discount_tax"]),
            "discount_total": float(order["discount_total"]),
            "status": str(order["status"]),
            "date_created": parser.parse(order["date_created"]).replace(tzinfo=tzinfo).isoformat(),
            "date_modified": parser.parse(order["date_modified"]).replace(tzinfo=tzinfo).isoformat(),
            "shipping_total": float(order["shipping_total"]),
            "shipping_tax": float(order["shipping_tax"]),
            "cart_tax": float(order["cart_tax"]),
            "prices_include_tax": order["prices_include_tax"],
            "customer_id": str(order["customer_id"]),
            "customer_ip_address": str(order["customer_ip_address"]),
            "customer_user_agent": str(order["customer_user_agent"]),
            "customer_note": str(order["customer_note"]),
            "coupon_lines": order["coupon_lines"],
            "total": float(order["total"]),
            "payment_method": str(order["payment_method"]),
            "payment_method_title": str(order["payment_method_title"]),
            "transaction_id": str(order["transaction_id"]),
            "new_customer": new_customer,

            "shipping_lines": order["shipping_lines"],
            "line_items": line_items,
            "fee_lines": order["fee_lines"],
            "refunds": order["refunds"],
            "shipping": order["shipping"],
            "_links": order["_links"]
        }

        if (order["date_completed"]):
            filtered["date_completed"] = parser.parse(order["date_completed"]).replace(tzinfo=tzinfo).isoformat()

        if (order["date_paid"]):
            filtered["date_paid"] = parser.parse(order["date_paid"]).replace(tzinfo=tzinfo).isoformat()

    except Exception as e:
        print(e)
        filtered = {}
    return filtered


def create_metadata_for_streams(schema, key_properties):
    mdata = [{"breadcrumb": [], "metadata": {
        "inclusion": "available", "forced-replication-method": "FULL_TABLE"}}]

    for key in schema["properties"]:
        if "object" in schema["properties"].get(key)["type"]:
            for prop in schema["properties"].get(key)["properties"]:
                inclusion = "automatic" if prop in key_properties else "available"
                mdata.extend([{
                    "breadcrumb": ["properties", key, "properties", prop],
                    "metadata": {"inclusion": inclusion}
                }])
        else:
            inclusion = "automatic" if key in key_properties else "available"
            mdata.append({"breadcrumb": ["properties", key], "metadata": {
                "inclusion": inclusion}})

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


def sync_orders(STATE, catalog):
    schema = load_schema("orders")
    singer.write_schema("orders", schema, ["order_id"])

    start = get_start(STATE, "orders", "last_update")
    LOGGER.info("Only syncing orders updated since " + start)
    last_update = start
    page_number = 1
    with metrics.record_counter("orders") as counter:
        params = {
            "after": last_update,
            "orderby": "date",
            "order": "asc",
            "per_page": "100",
            "page": page_number
        }
        while True:
            orders = _api_wrapper.orders(params)

            for order in orders:
                counter.increment()
                order = filter_order(order)
                if ("date_created" in order) and (
                        parser.parse(order["date_created"]).replace(tzinfo=None) > parser.parse(last_update).replace(
                        tzinfo=None)):
                    last_update = order["date_created"]
                singer.write_record("orders", order)
            if len(orders) < 100:
                break
            else:
                page_number += 1
    STATE = singer.write_bookmark(STATE, 'orders', 'last_update', last_update)
    singer.write_state(STATE)
    LOGGER.info("Completed Orders Sync")
    return STATE


def sync_reports(STATE, catalog):
    schema = load_schema("reports")
    singer.write_schema("reports", schema, ["store_url"])

    start = get_start_for_report(STATE, "reports", "date_end")
    LOGGER.info("Only syncing reports updated since " + start)
    last_update = start
    with metrics.record_counter("reports") as counter:
        while True:
            report_response = _api_wrapper.reports(last_update, last_update)
            counter.increment()
            report = filter_report(report_response[0], last_update)
            singer.write_record("reports", report)
            if pendulum.parse(last_update).to_date_string() == pendulum.yesterday().to_date_string():
                break
            last_update = pendulum.parse(last_update).add(days=1).to_date_string()
    STATE = singer.write_bookmark(STATE, 'reports', 'date_end', last_update)
    singer.write_state(STATE)
    LOGGER.info("Completed Reports Sync")
    return STATE


def sync_customers(STATE, catalog):
    schema = load_schema("customers")
    singer.write_schema("customers", schema, ["id"])

    start = get_start(STATE, "customers", "last_update")
    LOGGER.info("Only syncing customers updated since " + start)
    last_update = start
    page_number = 1
    with metrics.record_counter("customers") as counter:
        params = {
            "role": "all",
            "orderby": "id",
            "order": "asc",
            "per_page": "100",
            "page": page_number
        }
        while True:
            customers = _api_wrapper.customers(params)

            for customer in customers:
                counter.increment()
                mdata = create_metadata_for_streams(schema, ["id"])
                customer = transform(customer, schema, metadata=mdata)
                singer.write_record("customers", customer)
            if len(customers) < 100:
                break
            else:
                page_number += 1
    LOGGER.info("Completed customers Sync")
    return STATE


@attr.s
class Stream(object):
    tap_stream_id = attr.ib()
    sync = attr.ib()


STREAMS = [
    Stream("orders", sync_orders),
    Stream("reports", sync_reports),
    Stream("customers", sync_customers)
]


def get_streams_to_sync(streams, state):
    '''Get the streams to sync'''
    current_stream = singer.get_currently_syncing(state)
    result = streams
    if current_stream:
        result = list(itertools.dropwhile(
            lambda x: x.tap_stream_id != current_stream, streams))
    if not result:
        raise Exception("Unknown stream {} in state".format(current_stream))
    return result


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


def do_sync(STATE, catalogs):
    '''Sync the streams that were selected'''
    remaining_streams = get_streams_to_sync(STREAMS, STATE)
    selected_streams = get_selected_streams(remaining_streams, catalogs)
    if len(selected_streams) < 1:
        LOGGER.info("No Streams selected, please check that you have a schema selected in your catalog")
        return

    LOGGER.info("Starting sync. Will sync these streams: %s", [stream.tap_stream_id for stream in selected_streams])

    for stream in selected_streams:
        LOGGER.info("Syncing %s", stream.tap_stream_id)
        singer.set_currently_syncing(STATE, stream.tap_stream_id)
        singer.write_state(STATE)

        try:
            catalog = [cat for cat in catalogs.streams if cat.stream == stream.tap_stream_id][0]
            STATE = stream.sync(STATE, catalog)
        except Exception as e:
            LOGGER.critical(e)
            raise e


def get_abs_path(path):
    '''Returns the absolute path'''
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_discovered_schema(stream):
    '''Attach inclusion automatic to each schema'''
    schema = load_schema(stream.tap_stream_id)
    for k in schema['properties']:
        schema['properties'][k]['inclusion'] = 'automatic'
    return schema


def discover_schemas():
    '''Iterate through streams, push to an array and return'''
    result = {'streams': []}
    for stream in STREAMS:
        LOGGER.info('Loading schema for %s', stream.tap_stream_id)
        result['streams'].append({'stream': stream.tap_stream_id,
                                  'tap_stream_id': stream.tap_stream_id,
                                  'schema': load_discovered_schema(stream)})
    return result


def do_discover():
    '''JSON dump the schemas to stdout'''
    LOGGER.info("Loading Schemas")
    json.dump(discover_schemas(), sys.stdout, indent=4)


@utils.handle_top_exception(LOGGER)
def main():
    '''Entry point'''
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    CONFIG.update(args.config)
    global _api_wrapper
    _api_wrapper = ApiWrapper(CONFIG["url"], CONFIG["consumer_key"], CONFIG["consumer_secret"])
    STATE = {}

    if args.state:
        STATE.update(args.state)
    if args.discover:
        do_discover()
    elif args.catalog:
        do_sync(STATE, args.catalog)
    else:
        LOGGER.info("No Streams were selected")


if __name__ == "__main__":
    main()
