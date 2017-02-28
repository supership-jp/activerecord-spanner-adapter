# -*- frozen_string_literal: true -*-
require 'json'

module ActiveRecord
  module ConnectionAdapters
    module Spanner
      module Quoting
        IDENTIFIERS_PATTERN = /\A[a-zA-Z][a-zA-Z0-9_]*\z/

        def quote_identifier(name)
          # https://cloud.google.com/spanner/docs/data-definition-language?hl=ja#ddl_syntax
          # raise ArgumentError, "invalid table name #{name}" unless IDENTIFIERS_PATTERN =~ name
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

        def _quote(value)
          case value
          when Symbol, String, ActiveSupport::Multibyte::Chars, Type::Binary::Data
            quote_string(value.to_s)
          when true
            quoted_true
          when false
            quoted_false
          when nil
            'NULL'
          when Numeric, ActiveSupport::Duration
            value.to_s
          when Type::Time::Value
            %Q["#{quoted_time(value)}"]
          when Date, Time
            %Q["#{quoted_date(value)}"]
          else
            raise TypeError, "can't quote #{value.class.name}"
          end
        end

        def quote_string(value)
          # Not sure but string-escape syntax in SELECT statements in Spanner
          # looks to be the one in JSON by observation.
          JSON.generate(value)
        end

        def quoted_true
          'true'
        end

        def quoted_false
          'false'
        end
      end
    end
  end
end
