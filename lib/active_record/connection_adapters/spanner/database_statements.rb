module ActiveRecord
  module ConnectionAdapters
    module Spanner
      module DatabaseStatements
        include ConnectionAdapters::DatabaseStatements

        def execute(stmt)
          case stmt
          when Spanner::DDL
            execute_ddl(stmt)
          else
            super(stmt)
          end
        end

        def exec_query(sql, name = 'SQL', binds = [], prepare: :ignored)
          case
          when binds.kind_of?(Hash)
            # do nothing
          when binds.respond_to?(:to_hash)
            binds = binds.to_hash
          else
            binds = binds.each_with_index.inject({}) {|b, (value, i)|
              # TODO(yugui) Also implement Arel visitor
              b["@p#{i}"] = value
              b
            }
          end

          log(sql, name, binds) do
            results = session.execute(sql, params: binds, streaming: false) 
            columns = results.types.map(&:first)
            rows = results.rows.map {|row|
              columns.map {|col| row[col] }
            }
            ActiveRecord::Result.new(columns.map(&:to_s), rows)
          end
        end
      end
    end
  end
end

