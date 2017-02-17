require 'test_helper'
require 'active_record'

class ActiveRecord::ConnectionAdapters::SpannerTest < Minitest::Test
  def test_that_it_has_a_version_number
    refute_nil ::ActiveRecord::ConnectionAdapters::Spanner::VERSION
  end

  def test_that_it_establishes_connection
    ActiveRecord::Base.establish_connection(
      adapter: 'spanner',
      name: 'yugui-experimental',
      keyfile: File.join(__dir__, '../../service-account.json'),
    )
  end
end
