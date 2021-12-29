import json
import apache_beam as beam
from google.cloud import pubsub_v1
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions

PROJECT_ID = "york-cdf-start"

DATASET_ID = "test_1"
USD_PAYMENT_HISTORY_TABLE = "usd_order_payment_history"
EUR_PAYMENT_HISTORY_TABLE = "eur_order_payment_history"
GBP_PAYMENT_HISTORY_TABLE = "gbp_order_payment_history"

ORDER_SUBSCRIPTION = "dataflow-project-orders-sub"
STOCK_UPDATE_TOPIC = "dataflow-order-stock-update"
PIPELINE_OPTIONS = PipelineOptions(streaming=True)

publisher = pubsub_v1.PublisherClient()
stock_update_topic_path = publisher.topic_path(PROJECT_ID, STOCK_UPDATE_TOPIC)
order_subscription_path = publisher.subscription_path(PROJECT_ID, ORDER_SUBSCRIPTION)

def IS_ORDER_USD(element):
    return element["order_currency"] == "USD"

def IS_ORDER_EUR(element):
    return element["order_currency"] == "EUR"

def IS_ORDER_GBP(element):
    return element["order_currency"] == "GBP"

class convert_bytes_to_dict(beam.DoFn):
    def process(self, element):
        yield json.loads(element.decode("UTF-8"))

class split_customer_name(beam.DoFn):
    def process(self, element):
        name_list = element['customer_name'].split(' ')
        element['customer_first_name'] = name_list[0]
        element['customer_last_name'] = name_list[1]
        yield element

class split_order_address(beam.DoFn):
    def process(self, element):
        comma_split = element['order_address'].split(',')
        building_number = None
        street_name = None
        city = None
        state = None
        zip_code = None

        military_address = None
        building_and_street = None

        # we have a normal address
        if len(comma_split) > 2:
            building_and_street = comma_split[0]
            city = comma_split[1]
            state_and_zip = comma_split[2]

        # we have a military address
        else:
            # https://faq.usps.com/s/article/How-Do-I-Address-Military-Mail
            military_address = element['order_address']

        if building_and_street:
            street_split = building_and_street.split(' ')
            if street_split[0].isnumeric():
                building_number = street_split[0]
                # using string slicing to remove the building number
                street_name = building_and_street[len(building_number):]
            else:
                street_name = building_and_street

            state_and_zip_split = state_and_zip.split(' ')
            zip_code = state_and_zip_split[len(state_and_zip_split) - 1]
            # using string slicing to remove the zip code
            state = state_and_zip[:len(zip_code) - 1]

        if military_address:
            military_address_split = military_address.split(', ')
            city_state_zip_split = military_address_split[1].split(' ')

            street_name = military_address_split[0].strip()
            city = city_state_zip_split[0].strip()
            state = city_state_zip_split[1].strip()
            zip_code = city_state_zip_split[2].strip()

        element['order_building_number'] = building_number.strip() if building_number else ''
        element['order_street_name'] = street_name.strip() if street_name else ''
        element['order_city'] = city.strip() if city else ''
        element['order_state_code'] = state.strip() if state else ''
        element['order_zip_code'] = zip_code.strip() if zip_code else ''

        yield element

class calculate_order_cost(beam.DoFn):
    def process(self, element):
        cost_total = element['cost_shipping'] + element['cost_tax']
        for item in element['order_items']:
            cost_total += item['price']

        element['cost_total'] = round(cost_total, 2)

        yield element

class generate_stock_update_message(beam.DoFn):
    def process(self, element):
        stock_message = {
            "order_id": element['order_id'],
            "item_list": []
        }

        for item in element['order_items']:
            stock_message['item_list'].append(item['id'])

        yield json.dumps(stock_message).encode('utf-8')

class generate_payment_record(beam.DoFn):
    def process(self, element):
        record = {
            "order_id": element["order_id"],
            "order_address": {
                "order_building_number": element['order_building_number'],
                "order_street_name": element['order_street_name'],
                "order_city": element['order_city'],
                "order_state_code": element['order_state_code'],
                "order_zip_code": element['order_zip_code'],
            },
            "customer_first_name": element["customer_first_name"],
            "customer_last_name": element["customer_last_name"],
            "customer_ip": element["customer_ip"],
            "cost_total": element["cost_total"],
        }

        yield record

if __name__ == '__main__':
    with beam.Pipeline(options=PIPELINE_OPTIONS) as pipeline:
        message = pipeline | beam.io.ReadFromPubSub(subscription=order_subscription_path)

        # transform data
        order = message | beam.ParDo(convert_bytes_to_dict())
        order_split_names = order | beam.ParDo(split_customer_name())
        order_split_address = order_split_names | beam.ParDo(split_order_address())
        order_priced = order_split_address | beam.ParDo(calculate_order_cost())

        # send stock update
        stock_update_message = order_priced | beam.ParDo(generate_stock_update_message())
        stock_update_message | beam.io.WriteToPubSub(topic=stock_update_topic_path, with_attributes=False)

        # filter order
        usd_payment_record = order_priced | beam.Filter(IS_ORDER_USD) | "usd payment" >> beam.ParDo(generate_payment_record())
        eur_payment_record = order_priced | beam.Filter(IS_ORDER_EUR) | "eur payment" >> beam.ParDo(generate_payment_record())
        gbp_payment_record = order_priced | beam.Filter(IS_ORDER_GBP) | "gbp payment" >> beam.ParDo(generate_payment_record())

        # update tables
        usd_payment_record | "usd_write" >> beam.io.WriteToBigQuery(
            bigquery.TableReference(
                projectId=PROJECT_ID,
                datasetId=DATASET_ID,
                tableId=USD_PAYMENT_HISTORY_TABLE
            )
        )
        eur_payment_record | "eur_write" >> beam.io.WriteToBigQuery(
            bigquery.TableReference(
                projectId=PROJECT_ID,
                datasetId=DATASET_ID,
                tableId=EUR_PAYMENT_HISTORY_TABLE
            )
        )
        gbp_payment_record | "gbp_write" >> beam.io.WriteToBigQuery(
            bigquery.TableReference(
                projectId=PROJECT_ID,
                datasetId=DATASET_ID,
                tableId=GBP_PAYMENT_HISTORY_TABLE
            )
        )
