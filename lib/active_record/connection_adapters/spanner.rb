require "activerecord-spanner-adapter/version"

module ActiveRecord
  module ConnectionAdapters
    module Spanner
      include ActiveRecordSpannerAdapter
    end
  end
end
