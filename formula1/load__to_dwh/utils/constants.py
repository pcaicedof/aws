db_redshift_properties = {
    "redshift_jdbc_url": "jdbc:redshift://formula1-workgroup.381491849275.us-east-1.redshift-serverless.amazonaws.com:5439/formula1_dwh",
    "tempdir": "s3a://orbidi-logs/temp-dir",
    "user": "pecafa",
    "password": "C41c3d01982",
    "schema": "public"
}

db_postgresql_properties = {
    "url": "jdbc:postgresql://localhost:5432/my_project",
    "user": "pecafa",
    "password": "c41c3d0",
    "driver": "org.postgresql.Driver",
    "schema": "formula1",
    "dbname": "my_project"
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