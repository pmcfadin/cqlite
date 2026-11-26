CREATE TYPE fix4627.tt (
    a int,
    b frozen<tuple<int, text, uuid>>
);

CREATE TABLE fix4627.t_tuple (
    id int,
    ck int,
    nt frozen<tuple<int, frozen<tuple<text, uuid>>>>,
    p frozen<tt>,
    tp frozen<tuple<int, text, uuid>>,
    lt list<frozen<tuple<int, text>>>,
    tu frozen<tuple<int, frozen<tt>>>,
    PRIMARY KEY (id, ck)
) WITH CLUSTERING ORDER BY (ck ASC);
