require 'pry'
require 'pry-byebug'

$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
require 'active_record/connection_adapters/spanner'

module ActiveRecord::ConnectionAdapters::Spanner::TestHelper
  private
  TEST_INSTANCE = "yugui-experimental"

  def establish_connection
    ActiveRecord::Base.establish_connection(
      adapter: 'spanner',
      project: 'pj-seneca',
      instance: TEST_INSTANCE,
      database: 'e1',
      keyfile: File.join(__dir__, 'service-account.json'),
    )
  end
end

require 'minitest/spec'
require 'minitest/autorun'
