module ActiveRecord
  module ConnectionAdapters
    module Spanner
      module Quoting
        IDENTIFIERS_PATTERN = /\A[a-zA-Z][a-zA-Z0-9_]*\z/

        def quote_identifier(name)
          # https://cloud.google.com/spanner/docs/data-definition-language?hl=ja#ddl_syntax
          raise ArgumentError, "invalid table name #{name}" unless IDENTIFIERS_PATTERN =~ name
          "`#{name}`"
        end

        alias quote_table_name quote_identifier
        alias quote_column_name quote_identifier

        private
        def _type_cast(value)
          # NOTE: Spanner APIs are strongly typed unlike typical SQL interfaces.
          # So we don't want to serialize the value into string unlike other adapters.
          case value
          when Symbol, ActiveSupport::Multibyte::Chars, Type::Binary::Data
            value.to_s
          else
            value
          end
        end
      end
    end
  end
end
