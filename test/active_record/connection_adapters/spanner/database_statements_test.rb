require 'test_helper'

class Product < ActiveRecord::Base
end

describe ActiveRecord::ConnectionAdapters::Spanner::SchemaStatements do
  include ActiveRecord::ConnectionAdapters::Spanner::TestHelper

  before do
    establish_connection

    Product.connection.send(:session).commit do |c|
      c.delete 'products'
    end
  end

  it 'stores a new record' do
    created = Product.create! \
      name: 'cucumber2light extractor',
      description: 'Extracts light from tasty cucumber',
      price: '1000'

    found = Product.find_by(name: 'cucumber2light extractor')
    found.must_be :==, created
  end
end
