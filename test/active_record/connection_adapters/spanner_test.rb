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

  def test_create_database
    establish_connection

    name = "testdb_#{Time.now.to_i}"
    ActiveRecord::Base.connection.create_database name
  ensure
    ActiveRecord::Base.connection.drop_database name
  end

  private
  TEST_INSTANCE = "yugui-experimental"

  def establish_connection
    ActiveRecord::Base.establish_connection(
      adapter: 'spanner',
      project: 'pj-seneca',
      instance: TEST_INSTANCE,
      database: 'e1',
      keyfile: File.join(__dir__, '../../service-account.json'),
    )
  end
end
