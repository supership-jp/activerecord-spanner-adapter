module ActiveRecord
  module ConnectionAdapters
    module Spanner
      module SchemaStatements
        def create_database(name, instance: nil, statements: [])
          conn = session
          instance ||= conn.instance_id
          conn.service.create_database(instance, name, statements: statements)
        end

        def drop_database(name, instance: nil)
          conn = session
          instance ||= conn.instance_id
          conn.service.drop_database(instance, name)
        end

        def execute(stmt)
        end
      end
    end
  end
end

