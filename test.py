import apache_beam as beam
import ast
from google.cloud import bigquery

SUBSCRIPTION_ID = 'projects/york-cdf-start/subscriptions/hf-order-data-sub'
table1 = "york-cdf-start.h_femrite_proj_1.usd_order_payment_history"
table2 = "york-cdf-start.h_femrite_proj_1.eur_order_payment_history"
table3 = "york-cdf-start.h_femrite_proj_1.gbp_order_payment_history"


class PrintValue(beam.DoFn):
    def process(self, element):
        element = element.decode()
        element = ast.literal_eval(element)
        print(element)
        return [element]


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
        if element["order_currency"] == 'USD':
            new_data1 = {
                "order_id": element["order_id"],
                "order_address": {"order_building_number": order_building_number,
                                  "order_street_name": order_street_name,
                                  "order_city": order_city,
                                  "order_state_code": order_state_code,
                                  "order_zip_code": order_zip_code},
                "customer_first_name": customer_first_name,
                "customer_last_name": customer_last_name,
                "customer_ip": element["customer_ip"],
                "cost_total": cost_total
            }
            print(new_data1)
            return[new_data1]
        elif element["order_currency"] == 'EUR':
            new_data2 = {
                "order_id": element["order_id"],
                "order_address": {"order_building_number": order_building_number,
                                  "order_street_name": order_street_name,
                                  "order_city": order_city,
                                  "order_state_code": order_state_code,
                                  "order_zip_code": order_zip_code},
                "customer_first_name": customer_first_name,
                "customer_last_name": customer_last_name,
                "customer_ip": element["customer_ip"],
                "cost_total": cost_total
            }
            print(new_data2)
            return[new_data2]
        elif element["order_currency"] == 'GBP':
            new_data3 = {
                "order_id": element["order_id"],
                "order_address": {"order_building_number": order_building_number,
                                  "order_street_name": order_street_name,
                                  "order_city": order_city,
                                  "order_state_code": order_state_code,
                                  "order_zip_code": order_zip_code},
                "customer_first_name": customer_first_name,
                "customer_last_name": customer_last_name,
                "customer_ip": element["customer_ip"],
                "cost_total": cost_total
            }
            print(new_data3)
            return[new_data3]


if __name__ == '__main__':
    o = beam.options.pipeline_options.PipelineOptions(streaming=True, save_main_session=True)
    with beam.Pipeline(options=o) as p:
        raw_data = p | beam.io.ReadFromPubSub(subscription=SUBSCRIPTION_ID)
        conv_data = raw_data | beam.ParDo(PrintValue())
        trans_data = conv_data | beam.ParDo(Transform())
