require 'test_helper'
require 'active_record'

class ActiveRecord::ConnectionAdapters::SpannerTest < Minitest::Test
  def test_that_it_has_a_version_number
    refute_nil ::ActiveRecord::ConnectionAdapters::Spanner::VERSION
  end

  def test_that_it_establishes_connection
    establish_connection
  end

  def test_connection_activeness
    establish_connection
    assert ActiveRecord::Base.connection.active?
  end

  private
  def establish_connection
    ActiveRecord::Base.establish_connection(
      adapter: 'spanner',
      project: 'pj-seneca',
      instance: 'yugui-experimental',
      database: 'e1',
      keyfile: File.join(__dir__, '../../service-account.json'),
    )
  end
end
