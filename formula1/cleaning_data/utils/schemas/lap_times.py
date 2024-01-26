lap_times_schema = {
    "file_type": "csv",
    "header": False,
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
            "lap": {
                "field_name": "lap",
                "field_type": "int"
            }
        },
        {
            "position": {
                "field_name": "position",
                "field_type": "int"
            }
        },
        {
            "time": {
                "field_name": "time",
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
