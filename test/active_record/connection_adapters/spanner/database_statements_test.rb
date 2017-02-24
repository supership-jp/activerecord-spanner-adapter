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
      price: 1000

    found = Product.find_by(name: 'cucumber2light extractor')
    found.must_be :==, created
  end

  it 'deletes an existing record' do
    created = Product.create! \
      name: 'ground-ploughing hog',
      description: 'Ploughs the ground for you',
      price: 1500

    created.destroy

    Product.exists?(created.id).must_be :==, false
  end

  it 'deletes existing records' do
    ids = (1..3).map do |i|
      created = Product.create! \
        name: "ground-ploughing hog #{i}",
        description: 'Ploughs the ground for you',
        price: 1500 + i
    end

    ids.shift

    Product.delete(ids)

    Product.count.must_be :==, 1
  end

  it 'deletes all the existing records' do
    10.times do |i|
      created = Product.create! \
        name: "ground-ploughing hog #{i}",
        description: 'Ploughs the ground for you',
        price: 1500 + i
    end

    Product.delete_all
    Product.first.must_be_nil
  end

  it 'deletes matching records if a condition given' do
    10.times do |i|
      created = Product.create! \
        name: "ground-ploughing hog #{i}",
        description: 'Ploughs the ground for you',
        price: 1500 + i
    end

    Product.where('MOD(price, 3) = :modulo', modulo: 1).delete_all
    Product.count.must_be :==, [0, 2, 3, 5, 6, 8, 9].size
    Product.all.each do |prod|
      (prod.price % 3).wont_be :==, 1
    end
  end
end
