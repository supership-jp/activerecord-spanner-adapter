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

        # A mixin which provides ways to resolve rvalues.
        module RightValueResolveable
          def visit_Arel_Nodes_BindParam(o)
            binds.shift
          end

          def visit_Array(o)
            o.map {|item| accept(item) }
          end

          def visit_Arel_Nodes_Casted(o)
            a = o.attribute
            if a.able_to_type_cast?
              a.type_cast_for_database(o.val)
            else
              o.val
            end
          end

          private
          # To be overridden
          def binds
            raise NotImplementedError
          end
        end

        # Converts ASTs of INSERT, UPDATE or DELETE statements into forms
        # convenient for DatabaseStatements#insert, #update and #delete.
        class MutationVisitor < ::Arel::Visitors::Visitor
          include RightValueResolveable

          def initialize(schema_reader, binds)
            super()
            @schema_reader = schema_reader
            @binds = binds
          end

          attr_reader :binds
          private :binds

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

            pk = @schema_reader.primary_key(table)
            ids = WhereVisitor.new(o.relation, pk, binds).accept(o.wheres[0])

            if ids
              return [table, ids]
            else
              return fallback_result
            end
          end

          def visit_Arel_Nodes_Values o
            o.expressions.map.with_index do |value|
              case value
              when ::Arel::Nodes::SqlLiteral
                raise NotImplementedError, "mutation with SQL literal is not supported"
              else
                accept(value)
              end
            end
          end
        end

        # Tries to convert where clause into a set of ids if it is simple enough.
        # Returns nil if the clause is not simple.
        class WhereVisitor < ::Arel::Visitors::Visitor
          include RightValueResolveable

          NOT_SIMPLE = nil

          def initialize(relation, pk, binds)
            super()
            @relation = relation
            @pk = pk
            @binds = binds
          end

          attr_reader :binds
          private :binds

          def visit_Arel_Nodes_And(o)
            if o.children.size == 1
              accept(o.left)
            else
              NOT_SIMPLE
            end
          end

          def visit_Arel_Nodes_Equality(o)
            if pk_cond?(o)
              accept(o.right)
            else
              NOT_SIMPLE
            end
          end

          def visit_Arel_Nodes_In(o)
            return nil unless pk_cond?(o)

            if o.kind_of?(Array) and o.empty?
              []
            else
              accept(o.right)
            end
          end

          def unsupported(o)
            return NOT_SIMPLE
          end

          alias visit_Arel_Nodes_Grouping unsupported
          alias visit_Arel_Nodes_NotIn unsupported
          alias visit_Arel_Nodes_Or unsupported
          alias visit_Arel_Nodes_NotEqual unsupported
          alias visit_Arel_Nodes_Case unsupported
          alias visit_Arel_Nodes_Between unsupported
          alias visit_Arel_Nodes_GreaterThanOrEqual unsupported
          alias visit_Arel_Nodes_GreaterThan unsupported
          alias visit_Arel_Nodes_LessThanOrEqual unsupported
          alias visit_Arel_Nodes_LessThan unsupported
          alias visit_Arel_Nodes_Matches unsupported
          alias visit_Arel_Nodes_DoesNotMatch unsupported

          private
          def pk_cond?(o)
            o.left.kind_of?(Arel::Attributes::Attribute) &&
              o.left.relation == @relation &&
              o.left.name == @pk
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
          table, target, wheres = MutationVisitor.new(self, type_casted_binds.dup).accept(arel.ast)

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
          elsif target.size > 1
            fake_sql = "DELETE FROM #{quote_column_name(table)} WHERE (primary-key) IN ?"
            keyset = target
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

