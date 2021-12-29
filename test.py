import apache_beam as beam
import json
import ast
from google.cloud import bigquery

SUBSCRIPTION_ID = 'projects/york-cdf-start/subscriptions/hf-order-data-sub'
OUT_TOPIC = 'projects/york-cdf-start/topics/dataflow-order-stock-update'
table_USD = "york-cdf-start:h_femrite_proj_1.usd_order_payment_history"
table_EUR = "york-cdf-start:h_femrite_proj_1.eur_order_payment_history"
table_GBP = "york-cdf-start:h_femrite_proj_1.gbp_order_payment_history"

def Is_USD(element):
    return element["order_currency"] == "USD"

def Is_EUR(element):
    return element["order_currency"] == "EUR"

def Is_GBP(element):
    return element["order_currency"] == "GBP"

class PrintValue(beam.DoFn):
    def process(self, element):
        element = element.decode()
        element = ast.literal_eval(element)
        print(element)
        yield element

class Transform(beam.DoFn):
    def process(self, element):
        customer_first_name = element["customer_name"].split()[0]
        customer_last_name = element["customer_name"].split()[1]
        order_building_number = element["order_address"].split()[0]
        street_name = (element["order_address"].split(', ')[0]).split()[1:]
        order_street_name = ' '.join(street_name)
        order_city = element["order_address"].split(', ')[1]
        state_zip = (element["order_address"].split(', ')[-1]).split()
        order_state_code = state_zip[0]
        order_zip_code = state_zip[1]
        order_cost = []
        for i in element["order_items"]:
            order_cost.append(i["price"])
        total = 0
        for t in order_cost:
            total += t
        cost_total = round(((element["cost_shipping"]) + (element["cost_tax"]) + total), 2)
        new_data = {
            "order_id": element["order_id"],
            "order_address": {
                "order_building_number": order_building_number,
                "order_street_name": order_street_name,
                "order_city": order_city,
                "order_state_code": order_state_code,
                "order_zip_code": order_zip_code},
            "customer_first_name": customer_first_name,
            "customer_last_name": customer_last_name,
            "customer_ip": element["customer_ip"],
            "cost_total": cost_total
        }
        print(new_data)
        yield new_data

class CreateMessage(beam.DoFn):
    def process(self, element):
        message = {
            "order_id": element["order_id"],
            "items": []
        }
        for item in element["order_items"]:
            message["items"].append(item["id"])
        print(message)
        yield message

if __name__ == '__main__':
    o = beam.options.pipeline_options.PipelineOptions(streaming=True, save_main_session=True)
    with beam.Pipeline(options=o) as p:
        raw_data = p | beam.io.ReadFromPubSub(subscription=SUBSCRIPTION_ID)
        conversion_data = raw_data | beam.ParDo(PrintValue())
        message_data = conversion_data | beam.ParDo(CreateMessage()) | \
                       beam.Map(lambda a: json.dumps(a).encode("utf-8")) | beam.io.WriteToPubSub(topic=OUT_TOPIC)
        USD_table = conversion_data | beam.Filter(Is_USD) | "USD payment" >> beam.ParDo(Transform()) | \
                    "USD Write" >> beam.io.WriteToBigQuery(table_USD)
        EUR_table = conversion_data | beam.Filter(Is_EUR) | "EUR payment" >> beam.ParDo(Transform()) | \
                    "EUR Write" >> beam.io.WriteToBigQuery(table_EUR)
        GBP_table = conversion_data | beam.Filter(Is_GBP) | "GBP payment" >> beam.ParDo(Transform()) | \
                    "GBP Write" >> beam.io.WriteToBigQuery(table_GBP)
