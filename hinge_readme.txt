Hi Ben,

I have created uploader script (upload_files_to_s3.py) which takes parameters to upload to bucket name files from local folder

There is shell script in EC2 (hinge_run.sh) which is running every 5 minutes as cron job.  It does 2 things:
1) Activate the virutal environment with modules required (psycopg2 and boto3)
2) Calls the main script (hinge_main.py)

hinge_main.py processes one file at a time and stops other instances of it to run using file locking mechanism
To scale, above can be changed by creating LIKE tables for those in staging schema based on each file processed

Since our focus is on relationship between the users (or players and subjects as called on log files), new dimension for relationship was created.
To keep the ingestion process quick, there are 2 records per connection_id (both users as user_from and user_to in both records)

Both fact tables fct_ratings and fct_rating_accumulate use connection_id to store the data.

fct_rating_accumulate is accumulating snapshot table which will allow to analyze most questions about timeline of relationship without heavy joins.

IAM roles are used to allow Redshift and EC2 to work with S3 and for EC2 to connect to Redshift.

Things still do do:
1) Logging (both on Linux and in DB)
2) Have used only S3, EC2, Redshift and Python (EMR, Airflow or Spark were not used for solution due to limits of free tier infrastructure)
3) Could have used Lambda or SQS for notification but wanted to implement polling mechanism using crontab
4) Could have used RDS to track progress and even store SQLs (could have created templated SQL statements and stored in some configuration file too)

Thanks for the opportunity to work on the mini-solution.

Regards,
Ronak
