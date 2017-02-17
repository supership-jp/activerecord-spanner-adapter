require 'active_record/connection_adapters/abstract_adapter'

module ActiveRecord
  module ConnectionHandling
    def spanner_connection(config)
      ConnectionAdapters::SpannerAdapter.new
    end
  end

  module ConnectionAdapters
    class SpannerAdapter < AbstractAdapter
    end
  end
end

