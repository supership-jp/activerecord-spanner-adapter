require 'google/cloud/spanner'

require 'active_record/connection_adapters/abstract_adapter'
require 'active_record/connection_adapters/spanner/client'
require 'active_record/connection_adapters/spanner/database_statements'
require 'active_record/connection_adapters/spanner/schema_creation'
require 'active_record/connection_adapters/spanner/schema_statements'
require 'active_record/connection_adapters/spanner/quoting'
require 'grpc'

module ActiveRecord
  module ConnectionHandling
    def spanner_connection(config)
      ConnectionAdapters::SpannerAdapter.new(nil, logger, config)
    end
  end

  module ConnectionAdapters
    # A Google Cloud Spanner adapter
    #
    # Options:
    # - project
    class SpannerAdapter < AbstractAdapter
      ADAPTER_NAME = 'Spanner'.freeze
      CLIENT_PARAMS = [:project, :keyfile, :scope, :timeout, :client_config].freeze
      ADAPTER_OPTS = (CLIENT_PARAMS + [:instance, :database]).freeze

      include Spanner::SchemaStatements
      include Spanner::DatabaseStatements
      include Spanner::Quoting

      def initialize(connection, logger, config)
        conn_params = config.symbolize_keys.slice(*ADAPTER_OPTS)
        connect(conn_params)
        super(connection, logger, config)
      end

      def schema_creation # :nodoc:
        Spanner::SchemaCreation.new self
      end

      def arel_visitor
        QueryVisitor.new(self)
      end

      def active?
        [
          ::GRPC::Core::ConnectivityStates::IDLE,
          ::GRPC::Core::ConnectivityStates::READY,
        ].include?(@conn.service.channel.connectivity_state)
      end

      def connect(params)
        client_params = params.slice(*CLIENT_PARAMS)
        @conn = Google::Cloud::Spanner.new(**client_params)
        @instance_id = params[:instance]
        @database_id = params[:database]
      end

      def disconnect!
        super
        release_client!
      end

      def prefetch_primary_key?(table_name = nil)
        true
      end

      def next_sequence_value(table_name = nil)
        require 'securerandom'
        SecureRandom.uuid
      end

      private
      attr_reader :conn

      def initialize_type_map(m) # :nodoc:
        register_class_with_limit m, %r(STRING)i, Type::String
        register_class_with_limit m, %r(BYTES)i, Type::Binary
        m.register_type %r[STRING(MAX)]i, Type::Text.new(limit: 10 * 1024**2)
        m.register_type %r[BYTES(MAX)]i, Type::Binary.new(limit: 10 * 1024**2)
        m.register_type %r[BOOL]i, Type::Boolean.new
        m.register_type %r[INT64]i, Type::Integer.new(limit: 8)
        m.register_type %r[FLOAT64]i, Type::Float.new(limit: 53)
        m.register_type %r[DATE]i, Type::Date.new
        m.register_type %r[TIMESTAMP]i, Type::DateTime.new
        # TODO(yugui) Support array and struct
      end

      def instance
        @instance ||= conn.instance(@instance_id)
        raise ActiveRecord::NoDatabaseError unless @instance

        @instance
      end

      def database
        return @db if @db

        @db = instance.database(@database_id)
        raise ActiveRecord::NoDatabaseError unless @db
        raise ActiveRecord::ConnectionNotEstablished,
          "database #{@db.database_path} is not ready" unless @db.ready?

        @db
      end

      def raw_client
        @raw_client ||= conn.client(@instance_id, @database_id)
      end

      def client
        @client ||= ConnectionAdapters::Spanner::InitialPhaseClient.new(raw_client)
      end

      def release_client!
        @client&.close
        @client = nil
      end

      #def reset_transaction
      #  # @transaction_manager = Spanner::TransactionManager.new(self, client)
      #end
    end
  end
end
