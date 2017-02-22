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
        t.timestamps
      end
    end
  end

  class AddChildTable < ActiveRecord::Migration[5.0]
    def change
      create_table :user_profiles do |t|
        t.references :principal
        t.string :name
        t.timestamps
      end
    end
  end

  it 'creates table as defined' do
    targets = [
      [
        AddRootTable,
        -> { ActiveRecord::Base.connection.tables.must_include 'principals' },
      ],
      [
        AddChildTable,
        -> { ActiveRecord::Base.connection.tables.must_include 'user_profiles' },
      ],
    ]
    targets.each do |migration, expectation|
      migration.migrate(:up)
      expectation.()
    end
    targets.reverse_each do |migration,|
      migration.migrate(:down)
    end
  end

  private
  def new_migration(&body)
    Class.new(ActiveRecord::Migration[5.0], &body)
  end
end

