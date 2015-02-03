-- Function: taxi.get_len_of_section(bigint)

-- DROP FUNCTION taxi.get_len_of_section(bigint);

CREATE OR REPLACE FUNCTION taxi.get_len_of_section(v_id bigint)
  RETURNS real AS
$BODY$
declare v_len real := 0;
declare v_way_id bigint;
declare v_section record;
declare v_line record;
declare v_start int;
declare v_end int;
begin
	select  way_id from taxi.section_way where section_id = v_id into v_way_id;
	--extract section's endpoint
	select *
	from
		taxi.sections
	where
		id = v_id
	into
		v_section
	;
	--locate start index
	select
		sequence_id+1
	from
		way_nodes T1
	where
		T1.way_id = v_way_id and
		T1.node_id = v_section.from_node
	order by
		sequence_id
	limit 1
	into
		v_start
	;
	--locate end index
	select
		sequence_id+1
	from
		way_nodes T1
	where
		T1.way_id = v_way_id and
		T1.node_id = v_section.to_node
	order by
		sequence_id desc
	limit 1
	into
		v_end
	;
	
	--sum the length of intermediate segments
	while v_start < v_end loop
		select 
			T1.geom startpoint,
			T2.geom endpoint
		from 
			nodes T1,
			nodes T2,
			ways T3
		where
			T3.id = v_way_id and
			T1.id = T3.nodes[v_start] and
			T2.id = T3.nodes[v_end]
		into
			v_line
		;
		v_start = v_start+1;
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
