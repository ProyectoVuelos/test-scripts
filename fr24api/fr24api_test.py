import logging
from pyfr24 import FR24API, configure_logging

configure_logging(level=logging.DEBUG, log_file="pyfr24_test.log")

api_key = os.environ.get("FR24_API_KEY")
api = FR24API(api_key)

flight_ids = ["39bebe6e", "39a84c3c", "39b845d8"]

for flight_id in flight_ids:
    try:
        output_dir = api.export_flight_data(
            flight_id,
            output_dir=f"batch_export/{flight_id}"
        )
        print(f"Exported {flight_id} to {output_dir}")
    except Exception as e:
        print(f"Error exporting {flight_id}: {e}")