require 'test_helper'

describe ActiveRecord::ConnectionAdapters::Spanner::SchemaStatements do
  include ActiveRecord::ConnectionAdapters::Spanner::TestHelper

  let(:db_name) do
    name = "testdb_#{Time.now.to_i}"
  end

  before do
    establish_connection(db_name)
    ActiveRecord::Base.connection.create_database db_name
  end

  after do
    ActiveRecord::Base.connection.drop_database db_name
  end

  class AddRootTable < ActiveRecord::Migration[5.0]
    def change
      create_table :principals do |t|
        t.string :email
        t.text :description
        t.timestamp
      end
    end
  end

  it 'creates table as defined' do
    AddRootTable.migrate(:up)
    ActiveRecord::Base.connection.tables.must_include 'principals'
    AddRootTable.migrate(:down)
  end

  private
  def new_migration(&body)
    Class.new(ActiveRecord::Migration[5.0], &body)
  end
end

