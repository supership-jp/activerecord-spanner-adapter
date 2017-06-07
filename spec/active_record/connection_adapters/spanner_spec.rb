require 'spec_helper'
require 'active_record'

describe ActiveRecord::ConnectionAdapters::Spanner do
  include ActiveRecord::ConnectionAdapters::Spanner::SpecHelper

  it 'has a version' do
    expect(ActiveRecord::ConnectionAdapters::Spanner::VERSION).not_to be_nil
  end

  it 'establishes a connection' do
    expect { establish_connection }.not_to raise_error
  end

  context 'once connection established' do
    before { establish_connection }

    it 'has an active connection' do
      expect(ActiveRecord::Base.connection).to be_active
    end

    it 'is able to create database' do 
      name = "testdb_#{Time.now.to_i}"
      begin
        expect { ActiveRecord::Base.connection.create_database name }.not_to raise_error
      ensure
        ActiveRecord::Base.connection.drop_database name
      end
    end
  end
end
