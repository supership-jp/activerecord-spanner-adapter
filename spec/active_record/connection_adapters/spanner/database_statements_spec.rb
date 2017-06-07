require 'spec_helper'

class Product < ActiveRecord::Base
end

describe ActiveRecord::ConnectionAdapters::Spanner::DatabaseStatements do
  include ActiveRecord::ConnectionAdapters::Spanner::SpecHelper

  let(:base_price) { 1500 }

  before do
    establish_connection

    Product.connection.send(:client).delete 'products'
  end

  before do
    @hogs = 10.times.map do |i|
      Product.create! \
        name: "ground-ploughing hog #{i}",
        description: 'Ploughs the ground for you',
        price: base_price
    end
  end

  context 'when looks up a record' do
    subject { Product.find_by(name: @hogs[0].name) }

    it { is_expected.not_to be_nil }
    it {
      is_expected.to have_attributes(
        name: @hogs[0].name,
        description: @hogs[0].description,
        price: @hogs[0].price,
      )
    }
  end

  context 'when stores a new record' do
    before do
      Product.create! \
        name: 'cucumber2light extractor',
        description: 'Extracts light from tasty cucumber',
        price: 1000
    end

    it 'increments the number of records' do
      expect(Product.count).to eq(@hogs.size + 1)
    end
  end

  context 'when deletes an existing record' do
    before { @hogs[0].destroy }


    it "does not find the record" do
      expect { Product.find(@hogs[0].id) }.to raise_error(ActiveRecord::RecordNotFound)
    end
  end

  context 'when deletes records by id' do
    let(:ids) { @hogs[1..2].map(&:id) }
    before { Product.delete(ids) }

    it 'decreases the number of records' do
      expect(Product.count).to eq(@hogs.size - ids.size)
    end
  end

  context 'when deletes all records' do
    before { Product.delete_all }

    it 'has no records' do
      expect(Product).not_to exist
    end
  end

  context 'when deletes records by condition' do
    before do
      Product.where(
        'MOD(CAST(SUBSTR(name, :pos) AS INT64), 3) = :modulo',
        pos: 'ground-ploughing hog '.length,
        modulo: 1,
      ).delete_all
    end

    let(:other_ids) do
      (0...10).reject {|i| i % 3 == 1 }
    end

    it 'decreases the number of records' do
      expect(Product.count).to eq(other_ids.size)
    end

    it 'does not find the records deleted' do
      expect(Product.all).to all(satisfy {|p| p.price % 3 != 1})
    end
  end

  context 'when updates a record' do
    let(:new_price) { 2500 }
    before do
      @hogs[0].price = new_price
      @retval = @hogs[0].save
    end

    it 'returns true' do
      expect(@retval).to be true
    end

    it 'finds out the record updated' do
      found = Product.find(@hogs[0].id)
      expect(found.price).to eq(new_price)
    end
  end

  context 'when updates records with a condition' do
    before do
      %w[ black brown red orange yellow green blue purple gray white ].each do |color|
        Product.create! \
          name: "spider silk cloth : #{color}",
          description: "well-dyed #{color} cloth of spider silk",
          price: base_price
      end
    end

    let(:new_price) { 1000 }

    before do
      Product.where('name LIKE ?', '% : blue').
        or(Product.where('name LIKE ?', '% : orange')).
        update(:all, price: 1000)
      # ActiveRecord::Relation.update_all generates SqlLiteral for SET clause in UPDATE statement.
      # So this adapter cannot support such statements.
    end

    it 'finds the records updated' do
      expect(Product.all).to all(satisfy {|prod|
        if prod.name.ends_with? 'blue' or prod.name.ends_with? 'orange'
          prod.price == new_price
        else
          prod.price == base_price
        end
      })
    end
  end

  context 'when repeatedly reads a record in a transaction' do
    let(:new_price) { 2000 }

    before { @id = @hogs[0].id }

    it 'reads the same value' do
      mu = Mutex.new
      cv = ConditionVariable.new
      read_first = false
      th = Thread.start do
        mu.synchronize {
          cv.wait(mu) until read_first
          loaded = Product.find(@id)
          loaded.price = new_price
          loaded.save!
        }
      end

      Product.transaction(isolation: {strong: true}) {
        mu.synchronize {
          loaded = Product.find(@id)
          expect(loaded.price).to eq(base_price)
          read_first = true
          cv.signal
        }

        th.join

        loaded = Product.find(@id)
        expect(loaded.price).to eq(base_price)
      }

      loaded = Product.find(@id)
      expect(loaded.price).to eq(new_price)
    end
  end

  context 'when updates some records in a transaction' do
    let(:new_price) { 2000 }
    before do
      mu = Mutex.new
      cv = ConditionVariable.new
      written = false

      th = Thread.start do
        mu.synchronize {
          cv.wait(mu) until written
          @prices_read = @hogs.map do |product|
            loaded = Product.find(product.id)
            loaded.price
          end
        }
      end

      Product.transaction {
        mu.synchronize {
          @hogs[0].price = new_price
          @hogs[0].save!
          written = true
          cv.signal
        }

        th.join

        @hogs[1..-1].each do |prod|
          prod.price = new_price
          prod.save!
        end
      }
    end

    it 'reads the old value before commit' do
      expect(@prices_read).to all(be == base_price)
    end

    it 'reads the new value after commit' do
      expect(Product.all.map(&:price)).to all(be == new_price)
    end
  end

  context 'when rollbacks a transaction' do
    let(:new_price) { 2000 }
    before { @product = @hogs[0] }
    before do
      Product.transaction {
        @product.price = new_price
        @product.save!

        raise ActiveRecord::Rollback
      }
    end

    it 'does not reads the change' do
      @product.reload
      expect(@product.price).to eq(base_price)
    end
  end
end
