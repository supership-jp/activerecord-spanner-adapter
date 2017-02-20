require 'google/cloud/spanner'

require 'active_record/connection_adapters/abstract_adapter'
require 'active_record/connection_adapters/spanner/schema_statements'

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

      def initialize(connection, logger, config)
        super(connection, logger, config)
        conn_params = config.symbolize_keys.slice(*ADAPTER_OPTS)
        connect(conn_params)
      end

      attr_reader :connection
      private :connection

      def active?
        !!@connection
      end

      def connect(params)
        client_params = params.slice(*CLIENT_PARAMS)
        client = Google::Cloud::Spanner.new(**client_params)
        db = client.database(params[:instance], params[:database])
        raise ActiveRecord::ConnectionNotEstablished, 
          "database #{db.database_path} is not ready" unless db.ready?
        @connection = db.session
      end
    end
  end
end

