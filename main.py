import apache_beam as beam
from google.cloud import bigquery
import ast
import json

SUBSCRIPTION_ID = "projects/york-cdf-start/subscriptions/pm_topic-sub" # my pub/sub
n_sub = "projects/york-cdf-start/subscriptions/dataflow-order-stock-update-sub" # sub for the project
n_topic = "projects/york-cdf-start/topics/dataflow-order-stock-update" # topic for the project
# BQ_Table = "york-cdf-start:p_moua_proj_1.testing" # testing table that stored data
USD_table = "york-cdf-start:p_moua_proj_1.usd_order_payment_history" # created table in bigquery
EUR_table = "york-cdf-start:p_moua_proj_1.eur_order_payment_history"
GBP_table = "york-cdf-start:p_moua_proj_1.gbp_order_payment_history"
currency = ["USD", "EUR", "GBP"] # put the currency into an array

# schema for how the table should be formatted
table_schema = {
    'fields': [
        {"name": "order_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "order_address", "type": "RECORD", "mode": "REPEATED",
         'fields': [
             {"name": "order_building_number", "type": "STRING", "mode": "NULLABLE"},
             {"name": "order_street_name", "type": "STRING", "mode": "NULLABLE"},
             {"name": "order_city", "type": "STRING", "mode": "NULLABLE"},
             {"name": "order_state_code", "type": "STRING", "mode": "NULLABLE"},
             {"name": "order_zip_code", "type": "STRING", "mode": "NULLABLE"}],
         },
        {"name": "order_currency", "type": "STRING", "mode": "NULLABLE"},
        {"name": "customer_first_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "customer_last_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "customer_ip", "type": "STRING", "mode": "NULLABLE"},
        {"name": "customer_time", "type": "STRING", "mode": "NULLABLE"},
        {"name": "cost_total", "type": "FLOAT", "mode": "NULLABLE"}]

}

# where all the data was transformed/decoded and put into a dictionary in order to read
class SplitName(beam.DoFn):
    new_dict = {}

    # generator
    def process(self, element):
        # finding sum of price, cost_shipping & cost_tax, price & id were in its own array
        order_items = element["order_items"]
        price = 0
        item_list = []
        i = 0
        while i < len(order_items):
            price += float(order_items[i]["price"])
            item_list.append(order_items[i]["id"])
            i += 1
        cost_total = sum([price, float(element["cost_shipping"]), float(element["cost_tax"])])

        # splitting variables away from their element []
        customer_first_name = element["customer_name"].split()[0]
        customer_last_name = element["customer_name"].split()[1]
        order_building_number = element["order_address"].split()[0]
        street_name = (element["order_address"].split(",")[0]).split()[1:]
        order_street_name = " ".join(street_name)
        order_city = element["order_address"].split(",")[1]
        order_state_code = (element["order_address"].split(",")[-1]).split()[-2]
        order_zip_code = (element["order_address"].split()[-1])

        # the new dictionary that holds my information, how i want it to look when printed
        new_dict = {
            "order_id": element["order_id"],
            "order_address": [{"order_building_number": order_building_number,
                              "order_street_name": order_street_name,
                              "order_city": order_city,
                              "order_state_code": order_state_code,
                              "order_zip_code": order_zip_code}],
            "order_currency": element["order_currency"],
            "customer_ip": element["customer_ip"],
            "customer_first_name": customer_first_name,
            "customer_last_name": customer_last_name,
            "customer_time": element["customer_time"],
            "cost_total": cost_total
        }
        # print(new_dict)
        yield new_dict # yield is used like return however the function returns a generator


# followed the partition example from apache beam documentation to split USD, EUR, & GBP from list
def by_currency(element, num_currency):
    return currency.index(element['order_currency'])

# this function helps me run the message to display order_list & item_list; got the idea from previous generator
class create_message(beam.DoFn):
    message = {}
    def process(self, element):
        all_items = element["order_items"]
        item_list = []
        i = 0
        while i < len(all_items):
            item_list.append(all_items[i]["id"])
            i += 1
        message = {
            "order_id": element["order_id"],
            "item_list": item_list
            }
        yield message

# prints the converted data
class PrintValue(beam.DoFn):
    def process(self, element):
        element = element.decode()
        element = ast.literal_eval(element)
        # print(element)
        return [element]


def run():
    o = beam.options.pipeline_options.PipelineOptions(streaming=True, save_main_session=True)
    with beam.Pipeline(options=o) as pipeline:
        raw_data = pipeline | 'Read in Message' >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION_ID)
        conv_data = raw_data | 'Print' >> beam.ParDo(PrintValue())

        # writes my message to pubsub & displays the output with beam.Map(print)
        m_psub = conv_data | 'Create Pubsub Output' >> beam.ParDo(create_message())
        conv_message = m_psub | 'Convert Message from Dict To Bytestring' >> beam.Map(lambda s: json.dumps(s).encode("utf-8"))
        conv_message | 'Publish data to Pubsub' >> beam.io.WriteToPubSub(topic=n_topic)
        conv_message | beam.io.ReadFromPubSub(subscription=n_sub) | beam.Map(print) # displays converted message to see if message displays correctly

        # partitions my converted data
        usd, eur, gbp = (
                        conv_data | 'Partition' >> beam.Partition(by_currency, len(currency))
        )

        # converts each partitioned data into the format that I want it
        t_USD = usd | 'Transform USD' >> beam.ParDo(SplitName())
        t_EUR = eur | 'Transform EUR' >> beam.ParDo(SplitName())
        t_GBP = gbp | 'Transform GBP' >> beam.ParDo(SplitName())

        # writes each currency into it's own table
        t_USD | 'Write to USD_table' >> beam.io.WriteToBigQuery(
                            USD_table,
                            schema=table_schema,
                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
        t_EUR | 'Write to EUR_table' >> beam.io.WriteToBigQuery(
                            EUR_table,
                            schema=table_schema,
                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
        t_GBP | 'Write to GBP_table' >> beam.io.WriteToBigQuery(
                            GBP_table,
                            schema=table_schema,
                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


if __name__ == "__main__":
    run()
