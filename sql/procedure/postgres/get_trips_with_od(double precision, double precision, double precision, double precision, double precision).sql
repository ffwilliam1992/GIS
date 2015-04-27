-- Function: taxi.get_trips_with_od(double precision, double precision, double precision, double precision, double precision)

-- DROP FUNCTION taxi.get_trips_with_od(double precision, double precision, double precision, double precision, double precision);

CREATE OR REPLACE FUNCTION taxi.get_trips_with_od(
    v_lng1 double precision,
    v_lat1 double precision,
    v_lng2 double precision,
    v_lat2 double precision,
    v_radius double precision DEFAULT 100)
  RETURNS SETOF bigint AS
$BODY$
begin
	return query
	select
		trip_id
	from(
		select distinct
			first_value(point) over w as s,
			last_value(point) over w as e,
			trip_id
		from
			taxi.gps_raw
		window w as (
			partition by
				trip_id
			order by
				timestamp
			range between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING
		)) as T
	where
		trip_id is not null and
		st_distance_sphere(s, st_point(v_lng1, v_lat1)) < v_radius and
		st_distance_sphere(e, st_point(v_lng2, v_lat2)) < v_radius
	;
end;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100
  ROWS 1000;
ALTER FUNCTION taxi.get_trips_with_od(double precision, double precision, double precision, double precision, double precision)
  OWNER TO tx;
