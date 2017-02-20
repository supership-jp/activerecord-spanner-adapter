module ActiveRecord
  module ConnectionAdapters
    module Spanner
      module SchemaStatements
        def create_database(name, instance: nil, statements: [])
          conn = connection
          instance ||= conn.instance_id
          conn.service.create_database(instance, name, statements: statements)
        end

        def drop_database(name, instance: nil)
          conn = connection
          instance ||= conn.instance_id
          conn.service.drop_database(instance, name)
        end
      end
    end
  end
end

