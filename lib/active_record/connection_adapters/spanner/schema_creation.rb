module ActiveRecord
  module ConnectionAdapters
    module Spanner
      class DDL < String; end

      class SchemaCreation < AbstractAdapter::SchemaCreation
        def visit_TableDefinition(o)
          pk = o.columns.find {|c| c.type == :primary_key }
          ddl = "#{super} PRIMARY KEY (#{quote_column_name(pk.name)})"
          DDL.new(ddl)
        end
      end
    end
  end
end

