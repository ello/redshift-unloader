require 'rubygems'
require 'redshift/client'
require 'sparkr'
require 'honeybadger'

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

def retention_interval
  ENV['RETENTION_INTERVAL'] || '365 days'
end

def prunable_for_table(schemaname, tablename)
  Redshift::Client.connection.exec(<<-EOF)[0]['count'].to_i
    SELECT COUNT(1) FROM "#{schemaname}"."#{tablename}" WHERE sent_at < (current_date - interval '#{retention_interval}')
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

def prune_table!(schema, table)
  row_count = prunable_for_table(schema, table)
  if row_count == 0
    puts "No prunable rows found for #{schema}.#{table} -  skipping deletion"
  else
    puts "Pruning #{row_count} rows for #{schema}.#{table}"
    Redshift::Client.connection.exec(<<-EOF)
      DELETE
        FROM #{schema}.#{table}
        WHERE sent_at < (current_date - interval '#{retention_interval}')
    EOF
  end
  row_count
end

namespace :redshift do
  task  :list_tables do
    Redshift::Client.establish_connection
    row_count = 0
    tables.each do |schema, table|
      next unless table_has_sent_at_column?(schema, table)
      name = "#{schema}.#{table}"
      prunable = prunable_for_table(schema, table)
      puts "#{name.ljust(50)} (#{prunable} prunable)}"
      row_count += prunable
    end
    puts "Could prune #{row_count} total rows across #{tables.size} tables"
  end

  task :unload_all do
    Redshift::Client.establish_connection
    row_count = 0
    tables.each do |schema, table|
      next unless table_has_sent_at_column?(schema, table)
      full_table_name = "#{schema}.#{table}"
      puts "Examining #{full_table_name}"
      row_count += prune_table!(schema, table)
    end
    puts "Pruned #{row_count} total rows across #{tables.size} tables"

    puts "Vacuuming..."
    Redshift::Client.connection.exec('VACUUM FULL')

    puts "Analyzing..."
    Redshift::Client.connection.exec('ANALYZE')
  end
end
