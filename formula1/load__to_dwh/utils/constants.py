from decouple import config

db_redshift_properties = {
    "redshift_jdbc_url": config('DWH_URL'),
    "tempdir": config('DWH_TEMP_DIR'),
    "user": config('DWH_USER'),
    "password": config('DWH_PASSWORD'),
    "schema": config('DWH_SCHEMA')
}

db_postgresql_properties = {
    "url": config('DB_URL'),
    "host": config('DB_HOST'),
    "port": config('DB_PORT'),
    "user": config('DB_USER'),
    "password": config('DB_PASSWORD'),
    "driver": config('DB_DRIVER'),
    "schema": config('DB_SCHEMA'),
    "dbname": config('DB_NAME')
}


processed_folder = '/home/pedrodev/Documents/pcf_repository/aws/formula1/cleaning_data/processed'


fact_races_query = """

    select res.result_id , cir.circuit_id , r.race_id , r.year, d.driver_id ,
    c.name constructor_name , res.position ,
    q.position q_position, res.laps,
    cast(replace(res.fastest_lap_speed, '\\N','0') as float) fastest_lap_speed,
    case
        when q.position = 1 and res.position = 1 then 1
        else 0
    end has_made_pole_and_won,
    case
        when res.position = 1 then 1
        else 0
    end has_won,
    case
        when res.position is null then 1
        else 0
    end has_abandoned,
    case
        when res.position is not null then 1
        else 0
    end has_finished
    from formula1.results res 
    join formula1.races r on res.race_id = r.race_id 
    join formula1.drivers d on res.driver_id = d.driver_id 
    join formula1.constructors c on res.constructor_id = c.constructor_id
    join formula1.circuits cir on cir.circuit_id = r.circuit_id 
    join formula1.qualifying q on res.race_id = q.race_id and res.driver_id = q.driver_id
"""

table_queries= {
    "fact_races_results":fact_races_query
}