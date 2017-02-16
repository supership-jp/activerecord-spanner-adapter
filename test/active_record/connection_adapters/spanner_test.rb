require 'test_helper'

class ActiveRecord::ConnectionAdapters::SpannerTest < Minitest::Test
  def test_that_it_has_a_version_number
    refute_nil ::ActiveRecord::ConnectionAdapters::Spanner::VERSION
  end
end
