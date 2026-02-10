import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


PROJECT_ID = "burnished-web-484613-t0"
TOPIC = "house-price-topic"


class TransformData(beam.DoFn):

    def process(self, element):

        data = json.loads(element.decode("utf-8"))

        price = data["price"]
        sqft = data["sqft_living"]

        data["price_per_sqft"] = price / sqft

        yield data


options = PipelineOptions(

    streaming=True,
    runner="DataflowRunner",
    project=PROJECT_ID,
    region="us-central1",
    temp_location="gs://shahidtemp/temp"
)


with beam.Pipeline(options=options) as p:

    (
        p

        | "Read from PubSub" >> beam.io.ReadFromPubSub(
            topic=f"projects/{PROJECT_ID}/topics/{TOPIC}"
        )

        | "Transform" >> beam.ParDo(TransformData())

        | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            f"{PROJECT_ID}:house_dataset.house_prices_stream",

            schema="""
            price:FLOAT,
            bedrooms:INTEGER,
            bathrooms:FLOAT,
            sqft_living:INTEGER,
            floors:INTEGER,
            price_per_sqft:FLOAT
            """,

            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )
