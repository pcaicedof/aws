pit_stops_schema = {
    "file_type": "json",
    "multiline": True,
    "fields":[
        {
            "raceId": {
                "field_name": "race_id",
                "field_type": "int"
            }
        },
        {
            "driverId": {
                "field_name": "driver_id",
                "field_type": "int"
            }
        },
        {
            "stop": {
                "field_name": "stop",
                "field_type": "int"
            }
        },
        {
            "lap": {
                "field_name": "lap",
                "field_type": "int"
            }
        },
        {
            "time": {
                "field_name": "time",
                "field_type": "time"
            }
        },
        {
            "duration": {
                "field_name": "duration",
                "field_type": "varchar"
            }
        },
        {
            "milliseconds": {
                "field_name": "milliseconds",
                "field_type": "int"
            }
        }  
    ]
}
