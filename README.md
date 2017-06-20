<img src="http://d324imu86q1bqn.cloudfront.net/uploads/user/avatar/641/large_Ello.1000x1000.png" width="200px" height="200px" />

# Redshift Unloader

This is a simple schedulable script that lets us unload (or reload) data from Redshift to S3 month-by-month.

It's useful for automatically archiving data outside of a given retention window (i.e. 1y). 


## Configuring

The script relies on a handful of environment variables - 

- `REDSHIFT_URL` - URL to use to connect to the target Redshift instance/database
- `REDSHIFT_ROLE_ARN` - an AWS IAM role attached to the cluster with access to the S3 bucket. See [the AWS docs](http://docs.aws.amazon.com/redshift/latest/dg/loading-data-access-permissions.html) for details on setting this up.
- `S3_BUCKET` - the bucket to unload data into
- `ONLY_TABLES` - (optional) comma-separated list of fully-qualified (i.e. schema.tablename) tables to limit unload operations to
- `UNLOAD_ONLY` - (optional) only unload data, don't delete it

With those set, you can run the following Rake tasks:

- `rake redshift:list_tables` - list all tables in the database along with the count of archivable rows in each
- `rake redshift:unload_all` - unload all tables in the database with eligible data to the specified S3 bucket

## Setup

You'll need a working Ruby 2.3 setup with Bundler

Once those are set up, you can install dependencies with `bundle install`.

## License
Redshift Unloader is released under the [MIT License](blob/master/LICENSE.txt)

## Code of Conduct
Ello was created by idealists who believe that the essential nature of all human beings is to be kind, considerate, helpful, intelligent, responsible, and respectful of others. To that end, we will be enforcing [the Ello rules](https://ello.co/wtf/policies/rules/) within all of our open source projects. If you don’t follow the rules, you risk being ignored, banned, or reported for abuse.
