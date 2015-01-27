-- Function: taxi.get_len_of_section(bigint)

-- DROP FUNCTION taxi.get_len_of_section(bigint);

CREATE OR REPLACE FUNCTION taxi.get_len_of_section(v_id bigint)
  RETURNS real AS
$BODY$
declare v_len real := 0;
declare v_way record;
declare v_section record;
declare v_line record;
declare v_way_nodes ways.nodes%type;

begin
	--extract section's endpoint
	select *
	from
		taxi.sections
	where
		id = v_id
	into
		v_section
	;
	--locate indices of endpoints in way's nodes
	select
		T1.way_id,
		T1.sequence_id+1 ind_begin,
		T2.sequence_id+1 ind_end
	from
		way_nodes T1,
		way_nodes T2
	where
		T1.way_id = T2.way_id and
		T1.node_id = v_section.from_node and
		T2.node_id = v_section.to_node
	into
		v_way
	;
	select 
		nodes
	from
		ways
	where
		id = v_way.way_id
	into
		v_way_nodes
	;
	--sum the length of intermediate segments
	while v_way.ind_begin < v_way.ind_end loop
		select 
			T1.geom startpoint,
			T2.geom endpoint
		from 
			nodes T1,
			nodes T2,
			ways T3
		where
			T3.id = v_way.way_id and
			T1.id = v_way_nodes[v_way.ind_begin] and
			T2.id = v_way_nodes[v_way.ind_end]
		into
			v_line
		;
		v_way.ind_begin = v_way.ind_begin+1;
		v_len =
			v_len 
			+ st_distance_sphere(v_line.startpoint, v_line.endpoint);
	end loop
	;
	return v_len;
end;
$BODY$
  LANGUAGE plpgsql IMMUTABLE STRICT
  COST 100;
ALTER FUNCTION taxi.get_len_of_section(bigint)
  OWNER TO tx;
