import json
from datetime import datetime, timedelta
from AdaptTimeOption import AdaptTimeOption
from Event import Event


class TripEvent(Event):
    def _init_(self, payload):
        super()._init_(payload)
        json_data = json.loads(self.payload)
        self.trip_id = json_data["trip_id"]
        #self.timestamp = datetime.fromisoformat(json_data["dropoff_datetime"]).timestamp() * 1000
        # Assuming json_data["dropoff_datetime"] is something like "2016-01-01T05:33:00.000Z"
        self.timestamp = datetime.fromisoformat(json_data["dropoff_datetime"].rstrip("Z")).timestamp() * 1000

    @staticmethod
    def adapt_time(event, adapt_time_option):
        if adapt_time_option == AdaptTimeOption.ORIGINAL:
            return event
        elif adapt_time_option == AdaptTimeOption.INVOCATION:
            return TripEvent.from_string_shift_origin(event.payload)
        elif adapt_time_option == AdaptTimeOption.INGESTION:
            return TripEvent.from_string_overwrite_time(event.payload)
        else:
            raise ValueError("Invalid adapt_time_option")

    @staticmethod
    def from_string_shift_origin(payload, time_delta):
        json_data = json.loads(payload)
        pickup_time = datetime.fromisoformat(json_data["pickup_datetime"])
        dropoff_time = datetime.fromisoformat(json_data["dropoff_datetime"])

        json_data["pickup_datetime"] = (pickup_time + time_delta).isoformat()
        json_data["dropoff_datetime"] = (dropoff_time + time_delta).isoformat()

        return TripEvent(json.dumps(json_data))

    @staticmethod
    def from_string_overwrite_time(payload):
        json_data = json.loads(payload)
        pickup_time = datetime.fromisoformat(json_data["pickup_datetime"])
        dropoff_time = datetime.fromisoformat(json_data["dropoff_datetime"])
        time_delta = datetime.now() - dropoff_time

        json_data["pickup_datetime"] = (pickup_time + time_delta).isoformat()
        json_data["dropoff_datetime"] = (dropoff_time + time_delta).isoformat()

        return TripEvent(json.dumps(json_data))
    
    def get_partition_key(self):
        return str(self.trip_id)
    
    def _lt_(self, other):
        return self.trip_id < other.trip_id
    
    def _hash_(self):
        return hash(self.payload)