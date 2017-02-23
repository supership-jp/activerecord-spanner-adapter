require 'test_helper'

class Product < ActiveRecord::Base
end

describe ActiveRecord::ConnectionAdapters::Spanner::SchemaStatements do
  include ActiveRecord::ConnectionAdapters::Spanner::TestHelper

  before do
    establish_connection
  end

  it 'stores a new record' do
    Product.create! \
      name: 'cucumber2light extractor',
      description: 'Extracts light from tasty cucumber',
      price: '1000'
  end
end
