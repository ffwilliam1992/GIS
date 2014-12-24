CREATE TABLE nodes (
    node_id bigint ,
    latitude integer ,
    longitude integer,
    changeset_id bigint ,
    visible tinyint ,
    timestamp timestamp ,
    tile bigint ,
    version bigint ,
    redaction_id integer
);
CREATE TABLE node_tags (
    node_id bigint ,
    version bigint NULL,
    k varchar(255),
    v varchar(255)
);
CREATE TABLE ways (
    way_id bigint ,
    changeset_id bigint ,
    timestamp timestamp ,
    version bigint ,
    visible tinyint ,
    redaction_id integer
);


CREATE TABLE way_nodes (
    way_id bigint ,
    node_id bigint ,
    version bigint ,
    sequence_id bigint 
);
CREATE TABLE way_tags (
    way_id bigint ,
    k varchar(255),
    v varchar(255),
    version bigint
);
CREATE TABLE relations (
    relation_id bigint ,
    changeset_id bigint ,
    timestamp timestamp ,
    version bigint ,
    visible tinyint,
    redaction_id integer
);

CREATE TABLE relation_members (
    relation_id bigint ,
    member_type varchar(32) ,
    member_id bigint ,
    member_role varchar(255),
    version bigint ,
    sequence_id integer 
);

CREATE TABLE relation_tags (
    relation_id bigint ,
    k varchar(255),
    v varchar(255),
    version bigint 
);
