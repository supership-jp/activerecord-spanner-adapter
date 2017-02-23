module ActiveRecord
  module ConnectionAdapters
    module Spanner
      module SchemaStatements
        include ConnectionAdapters::SchemaStatements

        NATIVE_DATABASE_TYPES = {
          primary_key: 'STRING(36)',
          string:      { name: 'STRING', limit: 255 },
          text:        { name: 'STRING', limit: 'MAX' },
          integer:     { name: 'INT64' },
          float:       { name: 'FLOAT64' },
          datetime:    { name: 'TIMESTAMP' },
          date:        { name: 'DATE' },
          binary:      { name: 'BYTES', limit: 'MAX' },
          boolean:     { name: 'BOOL' },
        }

        def native_database_types  # :nodoc:
          NATIVE_DATABASE_TYPES
        end

        def tables
          # https://cloud.google.com/spanner/docs/information-schema
          select_values(<<-SQL, 'SCHEMA')
            SELECT
              t.table_name
            FROM
              information_schema.tables AS t
            WHERE
              t.table_catalog = '' AND t.table_schema = ''
          SQL
        end

        def views
          []
        end

        def indexes(table, name = :ignored)
          params = {table: table}
          results = exec_query(<<-"SQL", 'SCHEMA', params, prepare: false)
            SELECT
              idx.index_name,
              idx.index_type,
              idx.parent_table_name,
              idx.is_unique,
              idx.is_null_filtered
            FROM
              information_schema.indexes AS idx
            WHERE
              idx.table_catalog = '' AND
              idx.table_schema = '' AND
              idx.table_name = @table
          SQL

          results.map do |row|
            col_params = { table: table, index: row['index_name'] }
            col_results = exec_query(<<-"SQL", 'SCHEMA', col_params, prepare: false)
              SELECT
                col.column_name,
                col.column_ordering
              FROM
                information_schema.index_columns AS col
              WHERE
                col.table_catalog = '' AND
                col.table_schema = '' AND
                col.table_name = @table AND
                col.index_name = @index
              ORDER BY
                col.ordinal_position
            SQL

            IndexDefinition.new(
              table,
              row['index_name'],
              row['is_unique'],
              col_results.map {|row| row['column_name'] },
              nil,  # length
              col_results.map {|row| row['column_ordering'] },
              nil,  # where
              row['index_type'],
            )
          end
        end

        def columns(table)
          params = {table: table}
          results = exec_query(<<-'SQL', 'SCHEMA', params, prepare: false)
            SELECT
              col.column_name,
              col.column_default,
              col.is_nullable,
              col.spanner_type
            FROM
              information_schema.columns AS col
            WHERE
              col.table_catalog = '' AND
              col.table_schema = '' AND
              col.table_name = @table
            ORDER BY
              col.ordinal_position
          SQL

          results.map do |row|
            Column.new(
              row['column_name'],
              row['column_default'],
              fetch_type_metadata(row['spanner_type']),
              row['is_nullable'],
              table,
            )
          end
        end

        def primary_keys(table_name)  # :nodoc:
          indexes(table_name).find {|index|
            index.type == 'PRIMARY_KEY'
          }.columns
        end

        def create_database(name, instance_id: nil, statements: [])
          service = instance.service
          job = service.create_database(instance_id || instance.instance_id, name,
                                        statements: statements)
          job.wait_until_done! unless job.done?
          raise_on_error(job)
        end

        def drop_database(name, instance_id: nil)
          service = instance.service
          service.drop_database(instance_id || instance.instance_id, name)
        end

        def drop_table(name, options = {})
          raise NotImplementedError, 'if_exists in drop_table' if options[:if_exists]
          raise NotImplementedError, 'force in drop_table' if options[:force]

          ddls = indexes(name).select {|index|
            index.type != 'PRIMARY_KEY'
          }.map {|index|
            "DROP INDEX #{index.name}"
          }

          ddls << "DROP TABLE #{quote_table_name(name)}"
          execute_ddl(*ddls)
        end

        def add_index(table_name, column_name, options = {})
          index_name, index_type, index_columns, index_options = add_index_options(table_name, column_name, options)
          execute_ddl(<<-"SQL")
            CREATE #{index_type} INDEX
              #{quote_column_name(index_name)}
            ON
              #{quote_table_name(table_name)} (#{index_columns})
              #{index_options}
          SQL
        end

        def execute_ddl(*ddls)
          log(ddls.join(";\n"), 'SCHEMA') do
            job = database.update(statements: ddls.map(&:to_str))
            job.wait_until_done! unless job.done?
            raise_on_error(job.grpc)
          end
        end

        private
        def raise_on_error(job)
          raise Google::Cloud::Error.from_error(job.error) if job.error?
        end
      end
    end
  end
end

