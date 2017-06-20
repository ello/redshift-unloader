require 'rubygems'
require 'redshift/client'
require 'sparkr'
require 'yaml'

STDOUT.sync = true

START_MONTH = Date.parse('2015-03-01').freeze

def tables
  if ENV['ONLY_TABLES']
    ENV['ONLY_TABLES'].split(',').map { |t| t.split('.') }
  else
    Redshift::Client.connection.exec(<<-EOF).to_a.map { |r| [ r['schemaname'], r['tablename'] ] }
        SELECT schemaname, tablename
        FROM pg_tables
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'mailchimp')
        AND tablename NOT IN ('users')
        ORDER BY schemaname, tablename
    EOF
  end
end

def date_distribution_for_table(schemaname, tablename)
  Redshift::Client.connection.exec(<<-EOF)
    SELECT DATE_TRUNC('month', sent_at) AS month, COUNT(1) AS ct FROM "#{schemaname}"."#{tablename}" GROUP BY 1 ORDER BY 1 ASC
  EOF
end

def archivable_for_table(schemaname, tablename, interval = '12 months')
  Redshift::Client.connection.exec(<<-EOF)[0]['count']
    SELECT COUNT(1) FROM "#{schemaname}"."#{tablename}" WHERE sent_at < (current_date - interval '#{interval}')
  EOF
end

def table_has_sent_at_column?(schema, table)
  Redshift::Client.connection.exec(<<-EOF)[0]['count'].to_i > 0
    SELECT count(1)
    FROM pg_attribute
    INNER JOIN pg_class ON pg_attribute.attrelid = pg_class.oid
    INNER JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
    WHERE
      pg_attribute.attname = 'sent_at'
      AND pg_class.relname = '#{table}'
      AND pg_namespace.nspname = '#{schema}'
  EOF
end

def redshift_role_arn
  ENV['REDSHIFT_ROLE_ARN']
end

def s3_bucket
  ENV['S3_BUCKET']
end

def unload_table!(schema, table)
  return 0 unless table_has_sent_at_column?(schema, table)
  month = START_MONTH
  row_count = 0
  while month < (Date.today - 365)
    row_count += unload_table_for_month!(schema, table, month)
    clear_table_for_month!(schema, table, month) unless ENV['UNLOAD_ONLY']
    month = month.next_month
  end
  puts "Unloaded #{row_count} total rows for #{schema}.#{table}"
  row_count
end

def row_count_for_month(schema, table, month)
  Redshift::Client.connection.exec(<<-EOF)[0]['count'].to_i
    SELECT count(1) FROM #{schema}.#{table}
    WHERE
      sent_at >= DATE_TRUNC('month', TO_DATE('#{month.strftime('%Y-%m-%d')}', 'YYYY-MM-DD'))
      AND sent_at < DATE_TRUNC('month', TO_DATE('#{month.next_month.strftime('%Y-%m-%d')}', 'YYYY-MM-DD'))
  EOF
end

def unload_table_for_month!(schema, table, month)
  row_count = row_count_for_month(schema, table, month)
  if row_count == 0
    puts "No rows found for #{schema}.#{table} within #{month.strftime('%Y-%m')} -  skipping unload"
  else
    s3_prefix = "s3://#{s3_bucket}/unload/#{schema}/#{table}/#{month.strftime('%Y-%m')}/"
    puts "Unloading #{row_count} rows for #{schema}.#{table} within #{month.strftime('%Y-%m')} to #{s3_prefix}"
    Redshift::Client.connection.exec(<<-EOF)
      UNLOAD ('
        SELECT * FROM #{schema}.#{table}
        WHERE
          sent_at >= DATE_TRUNC(\\'month\\', TO_DATE(\\'#{month.strftime('%Y-%m-%d')}\\', \\'YYYY-MM-DD\\'))
          AND sent_at < DATE_TRUNC(\\'month\\', TO_DATE(\\'#{month.next_month.strftime('%Y-%m-%d')}\\', \\'YYYY-MM-DD\\'))
      ')
      TO '#{s3_prefix}'
      IAM_ROLE '#{redshift_role_arn}'
      MANIFEST GZIP ADDQUOTES ESCAPE PARALLEL ON
    EOF
  end
  row_count
end

def clear_table_for_month!(schema, table, month)
  row_count = row_count_for_month(schema, table, month)
  if row_count == 0
    puts "No rows found for #{schema}.#{table} within #{month.strftime('%Y-%m')} -  skipping deletion"
  else
    puts "Clearing #{row_count} rows for #{schema}.#{table} within #{month.strftime('%Y-%m')}"
    Redshift::Client.connection.exec(<<-EOF)
      DELETE
        FROM #{schema}.#{table}
        WHERE
          sent_at >= DATE_TRUNC(\\'month\\', TO_DATE(\\'#{month.strftime('%Y-%m-%d')}\\', \\'YYYY-MM-DD\\'))
          AND sent_at < DATE_TRUNC(\\'month\\', TO_DATE(\\'#{month.next_month.strftime('%Y-%m-%d')}\\', \\'YYYY-MM-DD\\'))
    EOF
  end
  row_count
end

namespace :redshift do
  task  :list_tables do
    Redshift::Client.establish_connection
    tables.each do |schema, table|
      name = "#{schema}.#{table}"
      puts "#{name.ljust(50)} (#{archivable_for_table(schema, table).to_s.ljust(10)} archivable) #{Sparkr.sparkline(date_distribution_for_table(schema, table).map { |r| r['ct'] } )}"
    end
  end

  task :unload_all do
    Redshift::Client.establish_connection
    row_count = 0
    tables.each do |schema, table|
      full_table_name = "#{schema}.#{table}"
      puts "Examining #{full_table_name}"
      row_count += unload_table!(schema, table)
    end
    puts "Unloaded #{row_count} total rows across #{tables.size} tables"

    puts "Vacuuming..."
    Redshift::Client.connection.exec('VACUUM FULL')

    puts "Analyzing..."
    Redshift::Client.connection.exec('ANALYZE')
  end
end
