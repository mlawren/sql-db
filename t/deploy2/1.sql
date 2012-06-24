CREATE TABLE actors (
   id integer primary key,
   name varchar
);

CREATE TABLE films (
   id integer primary key,
   title varchar
);

CREATE TABLE film_actors (
    actor_id integer references actors(id),
    film_id integer references films(id),
    primary key (actor_id,film_id)
);
