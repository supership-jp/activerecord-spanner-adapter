require 'test_helper'

class Product < ActiveRecord::Base
end

describe ActiveRecord::ConnectionAdapters::Spanner::SchemaStatements do
  include ActiveRecord::ConnectionAdapters::Spanner::TestHelper

  before do
    establish_connection

    Product.connection.send(:client).commit do |c|
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

  it 'updates an existing record' do
    created = Product.create! \
      name: 'spider silk cloth',
      description: 'well-dyed cloth of spider silk',
      price: 2000

    created.price = 2500
    created.save.must_equal true

    Product.find(created.id).price.must_equal 2500
  end

  it 'updates matching records if a condition given' do
    %w[ black brown red orange yellow green blue purple gray white ].each do |color|
      created = Product.create! \
        name: "spider silk cloth : #{color}",
        description: "well-dyed #{color} cloth of spider silk",
        price: 1500
    end

    Product.where('name LIKE ?', '% : blue').
      or(Product.where('name LIKE ?', '% : orange')).
      update(:all, price: 1000)
    # ActiveRecord::Relation.update_all generates SqlLiteral for SET clause in UPDATE statement.
    # So this adapter cannot support such statements.

    Product.all.each do |prod|
      if prod.name.ends_with? 'blue' or prod.name.ends_with? 'orange'
        prod.price.must_equal 1000
      else
        prod.price.must_equal 1500
      end
    end
  end
end
