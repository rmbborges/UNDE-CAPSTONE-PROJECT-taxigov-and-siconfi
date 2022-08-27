//// -- LEVEL 1
//// -- Schemas, Tables and References

// Creating tables
// You can define the tables with full schema names
Table dim_rides {
  id int
  request_id int
  started_at timestamp 
  ended_at timestamp
  pickup_latitude decimal
  pickup_longitude decimal
  dropoff_latitude decimal
  dropoff_longitude decimal
  distance decimal
  cost decimal
  Indexes {
    (id) [pk]
  }
}

// If schema name is omitted, it will default to "public" schema.
Table dim_requests as X {
  id int [pk, increment] // auto-increment
  requested_by varchar
  requested_at timestamp
  approved_at timestamp
  reason timestamp
  status varchar
  requested_dropoff_latitude varchar 
  requested_dropoff_longitude varchar
  commentary varchar
  
}

// If schema name is omitted, it will default to "public" schema.
Table dim_dates as D {
  id int [pk] // auto-increment
  timestamp timestamp 
  date date
  day int
  month int
  year int 
  day_of_week int 
  is_weekday boolean
}

// If schema name is omitted, it will default to "public" schema.
Table fact_daily_rides as F1 {
  id int [pk] // auto-increment
  date date
  ride_requests_count int 
  approved_ride_requests_count int 
  total_rides_cost decimal 
  total_rides_distance decimal
  average_cost_per_distance decimal
  average_approval_sla decimal
  average_ride_duration decimal
  average_rides_cost decimal
  average_rides_distance decimal
}





Ref: "dim_requests"."id" <> "dim_rides"."request_id"



Ref: "dim_dates"."date" < "fact_daily_rides"."date"