﻿-- Function: taxi.generate_trips_od_grid(double precision)

-- DROP FUNCTION taxi.generate_trips_od_grid(double precision);

CREATE OR REPLACE FUNCTION taxi.generate_trips_od_grid(v_delta double precision)
  RETURNS void AS
$BODY$
declare v_xmin taxi.bounds.x_min%type;
declare v_ymin taxi.bounds.y_min%type;
declare v_xmax taxi.bounds.x_max%type;
declare v_ymax taxi.bounds.y_max%type;
declare v_cur no scroll cursor for
	select
		id, o_point, d_point
	from
		taxi.trips_od
;
begin
	truncate table taxi.trips_od_grid;
	for v_record in v_cur
	loop
		insert into taxi.trips_od_grid
		select
			v_record.id, 
			taxi.get_grid(st_x(v_record.o_point), st_y(v_record.o_point), v_delta), 
			taxi.get_grid(st_x(v_record.d_point), st_y(v_record.o_point), v_delta)
	;
	end loop;
end;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION taxi.generate_trips_od_grid(double precision)
  OWNER TO tx;
