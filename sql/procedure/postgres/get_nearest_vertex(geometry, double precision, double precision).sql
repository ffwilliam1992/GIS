-- Function: taxi.get_nearest_vertex(geometry, double precision, double precision)

-- DROP FUNCTION taxi.get_nearest_vertex(geometry, double precision, double precision);

CREATE OR REPLACE FUNCTION taxi.get_nearest_vertex(
    v_point geometry,
    v_x double precision,
    v_y double precision)
  RETURNS bigint AS
$BODY$
declare v_id bigint;
begin
	select
		id
	from
		taxi.edges_vertices_pgr
	where
		(st_x(the_geom)-st_x(v_point))*v_x
		+(st_y(the_geom)-st_y(v_point))*v_y > 0.1
	order by
		st_distance_sphere(v_point, the_geom)
	limit 1
	into v_id
	;
	return v_id
	;
end;
$BODY$
  LANGUAGE plpgsql IMMUTABLE STRICT
  COST 100;
ALTER FUNCTION taxi.get_nearest_vertex(geometry, double precision, double precision)
  OWNER TO tx;
