﻿-- Function: taxi.get_aligned_segments(timestamp without time zone, timestamp without time zone)

-- DROP FUNCTION taxi.get_aligned_segments(timestamp without time zone, timestamp without time zone);

CREATE OR REPLACE FUNCTION taxi.get_aligned_segments(
    v_begin timestamp without time zone,
    v_end timestamp without time zone DEFAULT now()
 )RETURNS table(id varchar(16), point geometry(Point,4326), segment_id bigint) AS
$BODY$
declare v_result record;
declare v_traces refcursor;
declare v_trace_record record;
declare v_pre_id taxi.gps_raw.id%type;
declare v_pre_trace geometry(Point,4326)[];
declare v_nxt_trace geometry(Point,4326)[];
declare v_pre_state boolean := false;
declare v_direction double precision[];
declare v_aligned_segment record;
begin
	for v_trace_record in
	select 
		row_number() over w,
		T.point,
		T.id, st_x(T.point) latitude,
		st_y(T.point) longitude,
		state, timestamp
	from 
		taxi.gps_raw T
	where
		timestamp between v_begin and v_end
	window w as (
		partition by 
			T.id 
		order by
			T.id, timestamp
	)
	loop
		if(v_trace_record.id <> v_pre_id) then
			v_pre_state = false;
			v_pre_trace = null;
			v_pre_id = v_trace_record.id;
		end if;	
		--discard traces whose boarding position is not determinate
		if (v_pre_state=false and
			v_trace_record.state and
			array_upper(v_pre_trace, 1) is null) then
			continue;
		--continuous uncarried state
		elseif (v_pre_state=false and v_trace_record.state=false) then
			if(array_upper(v_pre_trace, 1) < 3 or 
				array_upper(v_pre_trace, 1) is null) then
				v_pre_trace := array_append(v_pre_trace, v_trace_record.point);
				raise notice 'trace %', array_length(v_pre_trace, 1);
			end if;
		--from uncarried to carried
		elseif (v_pre_state=false and v_trace_record.state) then
			v_pre_trace = array_append(v_pre_trace, v_trace_record.point);
			select * from taxi.get_direction(v_pre_trace) into v_direction;
			for v_aligned_segment in
			select taxi.get_aligned_segments(v_trace_record.point, v_direction[1], v_direction[2])
			loop
				return query
				select 
					v_trace_record.id, v_trace_record.point,
					v_aligned_segment.id;
				exit;
			end loop;
			v_pre_state = true;
		--continuous carried state
		elseif (v_pre_state and v_trace_record.state) then
			v_pre_trace = v_pre_trace[2:array_length(v_pre_trace, 1)];
			v_pre_trace = array_append(v_pre_trace, v_trace_record.point);
			select * from taxi.get_direction(v_pre_trace) into v_direction;
			for v_aligned_segment in
			select taxi.get_aligned_segments(v_trace_record.point, v_direction[1], v_direction[2])
			loop
				return query
				select 
					v_trace_record.id, v_trace_record.point,
					v_aligned_segment.id
				;
				exit;
			end loop;
		--from carried to uncarryied	
		elseif (v_pre_state and v_trace_record.state=false) then
			v_pre_trace = v_pre_trace[1:0];
			v_pre_state = false;
		end if;
		
	end loop;

	--return v_result;
end;$BODY$
  LANGUAGE plpgsql IMMUTABLE
  COST 100;
ALTER FUNCTION taxi.get_aligned_segments(timestamp without time zone, timestamp without time zone)
  OWNER TO tx;
--select taxi.get_aligned_segments('"2008-06-10 15:58:07"')
