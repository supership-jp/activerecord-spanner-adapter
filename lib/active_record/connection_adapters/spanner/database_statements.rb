module ActiveRecord
  module ConnectionAdapters
    module Spanner
      module DatabaseStatements
        include ConnectionAdapters::DatabaseStatements

        class QueryVisitor < ::Arel::Visitors::ToSql
          def visit_Arel_Nodes_BindParam(o, collector)
            collector.add_bind(o) {|bind_idx| "@p#{bind_idx}" }
          end
        end

        class MutationVisitor < ::Arel::Visitors::Visitor
          def initialize(schema_reader, binds)
            super()
            @schema_reader = schema_reader
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

          def visit_Arel_Nodes_DeleteStatement(o)
            table = o.relation.name

            # fallback_result lets the caller query the target id set at first and then
            # delete the ids.
            fallback_result = [table, nil, o.wheres]

            case o.wheres.size
            when 0
              return [table, :all]
            when 1
              # it might be a simple "id = ?". Let's check later
            else
              return fallback_result
            end

            # Returns fallback_result unless o.wheres is one of the followings
            # (1)
            #   and:
            #   - equality:
            #     left:
            #       attribute:
            #         relation: o.relation
            #         name: primary key
            #     right:
            #       bind_param:
            #
            # (2)
            #   equality:
            #    left:
            #      attribute:
            #        relation: o.relation
            #        name: primary key
            #    right:
            #      bind_param:
            #
            cond = o.wheres[0]
            if cond.kind_of?(Arel::Nodes::And)
              return fallback_result unless cond.children.size == 1
              cond = cond.left
            end

            return fallback_result unless \
              cond.kind_of?(Arel::Nodes::Equality) and
              cond.left.kind_of?(Arel::Attributes::Attribute)

            attr = cond.left
            pk = @schema_reader.primary_key(table)
            return fallback_result unless attr.relation == o.relation and attr.name == pk

            ids = [accept(cond.right)]
            return [table, ids]
          end

          def visit_Arel_Nodes_Values o
            o.expressions.map.with_index do |value|
              case value
              when ::Arel::Nodes::SqlLiteral
                raise NotImplementedError, "mutation with SQL literal is not supported"
              when ::Arel::Nodes::BindParam
                accept(value)
              else
                value
              end
            end
          end

          def visit_Arel_Nodes_BindParam(o)
            @binds.shift
          end
        end

        def insert(arel, name = nil, pk = nil, id_value = nil, sequence_name = nil, binds = [])
          raise NotImplementedError, "INSERT in raw SQL is not supported" unless arel.respond_to?(:ast)

          type_casted_binds = binds.map {|attr| type_cast(attr.value_for_database) }
          table, columns, values = MutationVisitor.new(self, type_casted_binds).accept(arel.ast)
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

        def delete(arel, name, binds)
          raise NotImplementedError, "DELETE in raw SQL is not supported" unless arel.respond_to?(:ast)

          type_casted_binds = binds.map {|attr| type_cast(attr.value_for_database) }
          table, target, wheres = MutationVisitor.new(self, type_casted_binds).accept(arel.ast)

          # TODO(yugui) Support composite primary key?
          pk = primary_key(table)
          if target.nil?
            where_clause = visitor.accept(wheres, collector).compile(binds.dup, self)
            target = select_values(<<~"SQL", name, binds)
              SELECT #{quote_column_name(pk)} FROM #{quote_table_name(table)} WHERE #{where_clause}
            SQL
          end

          if target == :all
            keyset = []
            fake_sql = "DELETE FROM #{quote_column_name(table)}"
          else
            fake_sql = "DELETE FROM #{quote_column_name(table)} WHERE (primary-key) = ?"
            keyset = target
          end

          log(fake_sql, name, binds) do
            session.commit do |c|
              c.delete(table, keyset)
            end
          end

          keyset.size
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
            spanner_binds = binds
            binds = binds.values
          when binds.respond_to?(:to_hash)
            spanner_binds = binds.to_hash
            binds = spanner_binds.values
          else
            spanner_binds = binds.each_with_index.inject({}) {|b, (attr, i)|
              b["p#{i+1}"] = type_cast(attr.value_for_database)
              b
            }
          end

          log(sql, name, binds) do
            results = session.execute(sql, params: spanner_binds, streaming: false) 
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

