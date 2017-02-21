require 'test_helper'
require 'active_record'

describe ActiveRecord::ConnectionAdapters::Spanner do
  it 'has version' do
    ActiveRecord::ConnectionAdapters::Spanner::VERSION.wont_be_nil
  end

  it 'establishes a connection' do
    establish_connection
  end

  it 'has an active connection' do
    establish_connection
    ActiveRecord::Base.connection.must_be :active?
  end

  it 'is able to create database' do 
    establish_connection

    name = "testdb_#{Time.now.to_i}"
    begin
      ActiveRecord::Base.connection.create_database name
    ensure
      ActiveRecord::Base.connection.drop_database name
    end
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
