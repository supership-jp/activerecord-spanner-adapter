module ActiveRecord
  module ConnectionAdapters
    module Spanner
      module SchemaStatements
        include ConnectionAdapters::SchemaStatements

        NATIVE_DATABASE_TYPES = {
          primary_key: 'BYTES(36)',
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
          execute_ddl("DROP TABLE #{quote_table_name(name)}")
        end

        IDENTIFIERS_PATTERN = /\A[a-zA-Z][a-zA-Z0-9_]*\z/

        def quote_table_name(name)
          # https://cloud.google.com/spanner/docs/data-definition-language?hl=ja#ddl_syntax
          raise ArgumentError, "invalid table name #{name}" unless IDENTIFIERS_PATTERN =~ name
          name
        end

        def execute_ddl(ddl)
          log(ddl, 'SCHEMA') do
            job = database.update(statements: [ddl.to_str])
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

