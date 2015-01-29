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
			taxi.get_od(v_record.id, v_begin, v_end) as (
				id character varying, 
				o_point geometry(Point, 4326), 
				o_time timestamp, 
				d_point geometry(Point, 4326), 
				d_time timestamp
		)
		;
		raise notice 'id %', v_record.id;
	end loop;
end;

$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION taxi.generate_trips_od(timestamp without time zone, timestamp without time zone)
  OWNER TO tx;
