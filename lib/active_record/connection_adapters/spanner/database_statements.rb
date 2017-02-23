module ActiveRecord
  module ConnectionAdapters
    module Spanner
      module DatabaseStatements
        include ConnectionAdapters::DatabaseStatements

        class MutationVisitor < ::Arel::Visitors::Visitor
          def initialize(binds)
            super()
            @binds = binds
          end

          def visit_Arel_Nodes_InsertStatement(o)
            raise NotImplementedError, 'INSERT INTO SELECT statement is not supported' if o.select
            table = o.relation.name
            columns = if o.columns.any?
                        o.columns.map(&:name)
                      else
                        columns(table).map(&:name)
                      end
            values = o.values ? accept(o.values) : []

            [table, columns, values]
          end

          def visit_Arel_Nodes_Values o
            bind_idx = 0
            o.expressions.map.with_index do |value|
              case value
              when ::Arel::Nodes::SqlLiteral
                raise NotImplementedError, "mutation with SQL literal is not supported"
              when ::Arel::Nodes::BindParam
                @binds[bind_idx].tap { bind_idx += 1 }
              else
                value
              end
            end
          end
        end

        def insert(arel, name = nil, pk = nil, id_value = nil, sequence_name = nil, binds = [])
          raise NotImplementedError, "INSERT in raw SQL is not supported" unless arel.respond_to?(:ast)

          type_casted_binds = binds.map {|attr| type_cast(attr.value_for_database) }
          table, columns, values = MutationVisitor.new(type_casted_binds).accept(arel.ast)
          fake_sql = <<~"SQL"
            INSERT INTO #{table}(#{columns.join(", ")}) VALUES (#{values.join(", ")})
          SQL

          row = columns.zip(values).inject({}) {|out, (col, value)|
            out[col] = value
            out
          }

          log(fake_sql, name) do
            session.commit do |c|
              c.insert table, row
            end
          end

          id_value
        end

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

