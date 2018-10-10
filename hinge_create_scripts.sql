CREATE TABLE staging.tbl_rating_file 
(
  rating_ts     TIMESTAMP,
  user_from     VARCHAR(40),
  user_to       VARCHAR(40),
  rating_type   INT
);

CREATE TABLE staging.l_dim_user
(
  user_name   VARCHAR(40)
)
distkey (user_name);

CREATE TABLE staging.l_connection_raw 
(
  connection_id bigint identity(0,1),
  user_from       VARCHAR(40),
  user_to         VARCHAR(40),
  user_string	  VARCHAR(80)
);


CREATE TABLE staging.l_connection_missing 
(
  connection_id   BIGINT,
  user_from       VARCHAR(40),
  user_to         VARCHAR(40)
);


CREATE TABLE staging.l_connection 
(
  connection_id   BIGINT,
  user_from       VARCHAR(40),
  user_to         VARCHAR(40)
);

CREATE TABLE staging.l_fct_ratings 
(
  rating_ts       TIMESTAMP,
  connection_id   BIGINT,
  rating_type     INT,
  user_from       VARCHAR(40)
)
distkey (connection_id);

CREATE TABLE dw.dim_user
(
  user_name   VARCHAR(40)
);

CREATE TABLE dw.dim_connection 
(
  connection_id   BIGINT,
  user_from       VARCHAR(40),
  user_to         VARCHAR(40)
)
distkey (connection_id);

CREATE TABLE dw.fct_ratings 
(
  rating_ts       TIMESTAMP,
  connection_id   BIGINT,
  rating_type     INT,
  user_from       VARCHAR(40)
)
distkey (connection_id);

CREATE TABLE dw.fct_rating_accumulate 
(
  connection_id       BIGINT,
  skip_cnt   INT,
  like_ts             TIMESTAMP,
  like_wo_comment_ts             TIMESTAMP,
  like_w_comment_ts             TIMESTAMP,
  match_ts            TIMESTAMP,
  non_report_terminal_ts TIMESTAMP,
  REPORT_TS TIMESTAMP,
  terminal_ts         TIMESTAMP
)
distkey (connection_id);



