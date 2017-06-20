<img src="http://d324imu86q1bqn.cloudfront.net/uploads/user/avatar/641/large_Ello.1000x1000.png" width="200px" height="200px" />

# Redshift Pruner

This is a simple schedulable script that lets us remove outdated [Segment Warehouse](https://segment.com/warehouses) data from Redshift.

It's useful for automatically archiving data outside of a given retention window (i.e. after 6 months). 


## Configuring

The script relies on a handful of environment variables - 

- `REDSHIFT_URL` - URL to use to connect to the target Redshift instance/database
- `ONLY_TABLES` - (optional) comma-separated list of fully-qualified (i.e. schema.tablename) tables to which to limit pruning operations
- `RETENTION_INTERVAL` - (optional, defaults to `365 days`) prune data older than this time ago, as specified by a [Postgres interval value](https://www.postgresql.org/docs/8.4/static/datatype-datetime.html)

With those set, you can run the following Rake tasks:

- `rake redshift:list_tables` - list all tables in the database along with the count of prunable rows in each
- `rake redshift:prune_all` - prune data older than the specified threshold from all tables in the database

## Setup

You'll need a working Ruby 2.3 setup with Bundler

Once those are set up, you can install dependencies with `bundle install`.

## License
Redshift Unloader is released under the [MIT License](blob/master/LICENSE.txt)

## Code of Conduct
Ello was created by idealists who believe that the essential nature of all human beings is to be kind, considerate, helpful, intelligent, responsible, and respectful of others. To that end, we will be enforcing [the Ello rules](https://ello.co/wtf/policies/rules/) within all of our open source projects. If you don’t follow the rules, you risk being ignored, banned, or reported for abuse.
