pit_stops_schema = {
    "file_type": "json",
    "multiline": True,
    "fields":[
        {
            "raceId": {
                "field_name": "driver_id",
                "field_type": "int"
            }
        },
        {
            "driverId": {
                "field_name": "driver_ref",
                "field_type": "int"
            }
        },
        {
            "stop": {
                "field_name": "number",
                "field_type": "int"
            }
        },
        {
            "lap": {
                "field_name": "code",
                "field_type": "int"
            }
        },
        {
            "time": {
                "field_name": "forename",
                "field_type": "time"
            }
        },
        {
            "duration": {
                "field_name": "surname",
                "field_type": "varchar"
            }
        },
        {
            "milliseconds": {
                "field_name": "driver_birthdate",
                "field_type": "int"
            }
        }  
    ]
}
