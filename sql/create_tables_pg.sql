drop table if exists taxi.segment_section;
drop table if exists taxi.segments;
drop table if exists taxi.section_way;
drop table if exists taxi.sections;
drop table if exists highway_types;
create table taxi.sections(
	id bigint primary key,
	from_node bigint,
	to_node bigint
);
create table taxi.section_way(
	section_id bigint references taxi.sections(id),
	way_id bigint
);
create table taxi.segments(
	id bigint primary key,
	from_node bigint,
	to_node bigint
);
create table taxi.segment_section(
	segment_id bigint references taxi.segments(id),
	section_id bigint
);
create table taxi.highway_types(
	id	int primary key,
	type varchar(16)
);
create table taxi.driving_highway_types(
	id int references taxi.highway_types(id)
);
insert into taxi.highway_types
select  row_number() over () , *
from (
	select  distinct tags->'highway' from ways where  tags?'highway'
) T
;
insert into taxi.driving_highway_types
select id
from
	taxi.highway_types
where
	type in ('living_street', 'motorway',
		'motorway_link',
		'primary', 'primary_link',
		'residential', 'secondary',
		'tertiary', 'trunk',
		'trunk_link','unclassified')
;
drop table if exists taxi.edges;
CREATE TABLE taxi.edges(
  id bigint NOT NULL,
  source bigint,
  target bigint,
  len double precision,
  rlen double precision,
  x1 double precision,
  y1 double precision,
  x2 double precision,
  y2 double precision,
  the_geom geometry(LineString,4326),
  maxspeed double precision,
  speed_kph double precision,
  cost_len double precision,
  cost_time double precision,
  rcost_len double precision,
  rcost_time double precision,
  to_cost double precision,
  rule text,
  isolated integer,
  CONSTRAINT edges_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);
