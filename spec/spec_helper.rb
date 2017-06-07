require 'pry'
require 'pry-byebug'

$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
require 'active_record/connection_adapters/spanner'

ActiveRecord::Base.logger = Logger.new(STDERR)

module ActiveRecord::ConnectionAdapters::Spanner::SpecHelper
  private
  TEST_INSTANCE = "yugui-experimental"

  def establish_connection(db_name = 'e1')
    ActiveRecord::Base.establish_connection(
      adapter: 'spanner',
      project: 'pj-seneca',
      instance: TEST_INSTANCE,
      database: db_name,
      keyfile: File.join(__dir__, 'service-account.json'),
    )
  end
end

RSpec.configure do |config|
end

