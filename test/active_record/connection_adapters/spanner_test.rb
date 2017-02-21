require 'test_helper'
require 'active_record'

describe ActiveRecord::ConnectionAdapters::Spanner do
  include ActiveRecord::ConnectionAdapters::Spanner::TestHelper

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
end
