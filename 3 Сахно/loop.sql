
select * from movies;
create table moviescopy as select * from movies; 
delete from moviescopy;
select * from moviescopy;


DO $$
 DECLARE
     name_m	      moviescopy.m_name%TYPE;
	 id_genrem    moviescopy.id_genre%TYPE;
     id_ratingm   moviescopy.id_rating%TYPE;
     id_runtimem  moviescopy.id_runtime%TYPE;

 BEGIN
 	 name_m := 'movies-';
	 id_genrem := 0;
	 id_ratingm := 10;
     id_runtimem := 1;
     FOR counter IN 1..10
         LOOP
            INSERT INTO moviescopy (m_name, id_genre, id_rating, id_runtime)
             VALUES (name_m || counter + 1,  counter + 1, id_ratingm + counter, counter + 1);
         END LOOP;
 END;
 $$