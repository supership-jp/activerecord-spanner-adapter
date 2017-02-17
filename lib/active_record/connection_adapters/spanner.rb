require "active_record"
require "active_record/connection_adapters/spanner_adapter"

module ActiveRecord
  module ConnectionAdapters
    module Spanner
      include ActiveRecordSpannerAdapter
    end
  end
end
