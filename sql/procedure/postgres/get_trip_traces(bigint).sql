-- Function: taxi.get_trip_traces(bigint)

-- DROP FUNCTION taxi.get_trip_traces(bigint);

CREATE OR REPLACE FUNCTION taxi.get_trip_traces(v_trip_id bigint)
  RETURNS SETOF bigint AS
$BODY$
declare v_pre record;
declare v_trace record;
declare v_pre_vertex bigint := -1;
declare v_cur_vertex bigint;
declare v_vertex bigint;
declare v_pgr_cost_result record;
begin
	select taxi.get_trip_vertices(v_trip_id)
	into v_pre_vertex 
	limit 1
	;
	for v_vertex in
	select *
	from
		(select taxi.get_trip_vertices(v_trip_id) v) T
	where
		v <> v_pre_vertex
	loop
		if(taxi.is_adjacent(v_pre_vertex, v_vertex)) then
			return query select id from taxi.edges where source = v_pre_vertex and target = v_vertex;
		else
			for v_pgr_cost_result in
			select seq, id1, id2, cost from pgr_dijkstra(
				'select id, source::integer, target::integer, len as cost, rlen reverse_cost from taxi.edges',
				v_pre_vertex::integer,
				v_vertex::integer,
				true,
				true
			)
			loop
				if(v_pgr_cost_result.id2 <> -1) then
					return next v_pgr_cost_result.id2;
				end if
				;
			end loop;
		end if;
		v_pre_vertex = v_vertex;
	end loop;
end;
$BODY$
  LANGUAGE plpgsql IMMUTABLE STRICT
  COST 100
  ROWS 1000;
ALTER FUNCTION taxi.get_trip_traces(bigint)
  OWNER TO tx;
