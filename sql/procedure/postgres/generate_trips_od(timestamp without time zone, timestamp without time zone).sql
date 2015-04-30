-- Function: taxi.generate_trips_od(timestamp without time zone, timestamp without time zone)

-- DROP FUNCTION taxi.generate_trips_od(timestamp without time zone, timestamp without time zone);

CREATE OR REPLACE FUNCTION taxi.generate_trips_od(
    v_begin timestamp without time zone,
    v_end timestamp without time zone DEFAULT now())
  RETURNS void AS
$BODY$
declare v_record record;
begin
	truncate table taxi.trips_od;
	insert into taxi.trips_od
	select distinct
		trip_id,
		id,
		first_value(point) over w o,
		first_value(timestamp) over w s,
		last_value(point) over w d,
		last_value(timestamp) over w e
	from
		taxi.gps_raw
	where 
		trip_id is not null and timestamp between v_begin and v_end
	window w as (
		partition by
			trip_id
		order by
			timestamp
		range between unbounded preceding and unbounded following
	);
	/*
	for v_record in
	select distinct
		id
	from
		taxi.gps_raw
	loop
		insert into
			taxi.trips_od(uid, o_point, o_time, d_point, d_time)
		select
			* 
		from 
			taxi.get_od(v_record.id, v_begin::timestamp, v_end::timestamp) as (
				id character varying, 
				o_point geometry(Point, 4326), 
				o_time timestamp, 
				d_point geometry(Point, 4326), 
				d_time timestamp
		)
		;
		raise notice 'id %', v_record.id;
	end loop;
	*/
end;

$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION taxi.generate_trips_od(timestamp without time zone, timestamp without time zone)
  OWNER TO tx;
