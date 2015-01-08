select * from ways where tags ? 'highway' and tags->'name'='江东中路' fetch first 10 rows only;
--checking sections
select taxi.generate_sections_segments();
select
	T2.way_id , T1.*, T3.nodes
from 
	taxi.sections T1,
	taxi.section_way T2,
	ways T3
where
	T1.id = T2.section_id and
	T2.way_id = T3.id
;
--checking segments
select
	T4.id,  T1.*, T4.nodes
from
	taxi.segments T1,
	taxi.segment_section T2,
	taxi.section_way T3,	
	ways T4
where
	T1.id = T2.segment_id and
	T2.section_id = T3.section_id and
	T3.way_id = T4.id
;
select 
	T2.id from_id, T3.id to_id,
	st_distance_sphere(T2.geom, T3.geom) distance
from 
	taxi.segments T1,
	nodes T2,
	nodes T3
where
	T1.from_node = T2.id and
	T1.to_node = T3.id
order by
	3 desc
;
select st_x(geom) from nodes fetch first 3 rows only;
	