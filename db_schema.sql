-- public.urls definition
-- Drop table
 DROP TABLE public.urls;
-- Create table
CREATE TABLE public.urls (
	url_id int4 NOT NULL,
	url varchar(100) NOT NULL,
	interval int4 NOT NULL,
	regex varchar(512) NULL
);
-- Create indexes
CREATE UNIQUE INDEX urls_logger_id_idx ON public.urls USING btree (url_id);
CREATE UNIQUE INDEX urls_logger_ip_idx ON public.urls USING btree (url);
-- Permissions
ALTER TABLE public.urls OWNER TO user;
GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE public.urls TO user;


-- public.stats definition
-- Drop table
DROP TABLE public.stats;
-- Create table
CREATE TABLE public.stats (
	url_id int4 NOT NULL,
	status int4 NOT NULL,
	request_ts timestamp(0) NOT NULL,
	responce_ts timestamp(0) NOT NULL,
	regex_mach varchar(515) NULL
);
-- Create indexes
CREATE UNIQUE INDEX stats_url_id_idx ON public.stats USING btree (url_id, request_ts);
-- Permissions
ALTER TABLE public.stats OWNER TO user;
GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE public.stats TO user;
