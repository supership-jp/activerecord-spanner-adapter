require 'test_helper'

describe ActiveRecord::ConnectionAdapters::Spanner::InitialPhaseClient do
  InitialPhaseClient = ActiveRecord::ConnectionAdapters::Spanner::InitialPhaseClient
  ReadWriteTransactionClient = ActiveRecord::ConnectionAdapters::Spanner::ReadWriteTransactionClient
  SnapshotClient = ActiveRecord::ConnectionAdapters::Spanner::SnapshotClient

  let(:example_table) { 'foo' }
  let(:example_rows) {
    [
      {id: 1, name: 'a'},
      {id: 2, name: 'b'},
    ]
  }
  let(:example_keyset) { [1, 2, 3] }
  let(:stub_client) {
    Object.new.instance_eval do
      @stub_transaction = Object.new.instance_eval do
        def safe_rollback; end
        def commit
          yield Google::Cloud::Spanner::Commit.new
        end

        self
      end

      def insert(*) true end
      def update(*) true end
      def delete(*) true end
      def execute(*) nil end

      def transaction(*)
        yield @stub_transaction
      end

      def snapshot(**options)
        stub = Object.new
        yield stub
      end

      self
    end
  }

  it 'inserts rows with the given client' do
    native = Minitest::Mock.new
    native.expect(:insert, true, [example_table, example_rows])

    client = InitialPhaseClient.new(native)
    client.insert(example_table, example_rows)
    native.verify
  end

  it 'inserts rows without changing transaction state' do
    client = InitialPhaseClient.new(stub_client)
    client.insert(example_table, example_rows)
    client.next.must_be :==, client
  end

  it 'updates rows with the given client' do
    native = Minitest::Mock.new
    native.expect(:update, true, [example_table, example_rows])

    client = InitialPhaseClient.new(native)
    client.update(example_table, example_rows)
    native.verify
  end

  it 'updates rows without changing transaction state' do
    client = InitialPhaseClient.new(stub_client)
    client.update(example_table, example_rows)
    client.next.must_be :==, client
  end

  it 'deletes rows with the given client' do
    native = Minitest::Mock.new
    native.expect(:delete, true, [example_table, example_keyset])

    client = InitialPhaseClient.new(native)
    client.delete(example_table, example_keyset)
    native.verify
  end

  it 'deletes rows without changing transaction state' do
    client = InitialPhaseClient.new(stub_client)
    client.delete(example_table, example_keyset)
    client.next.must_be :==, client
  end

  it 'does not support committing transactions' do
    client = InitialPhaseClient.new(stub_client)
    assert_raises(ActiveRecord::ConnectionAdapters::Spanner::TransactionStateError) do
      client.commit
    end
  end

  it 'does not support rolling transactions back' do
    client = InitialPhaseClient.new(stub_client)
    assert_raises(ActiveRecord::ConnectionAdapters::Spanner::TransactionStateError) do
      client.rollback
    end
  end

  it 'transit to read phase on begining r/w transaction' do 
    client = InitialPhaseClient.new(stub_client)
    client.begin_transaction
    client.next.must_be :kind_of?, ReadWriteTransactionClient
  end

  it 'transit to snapshot phase on begining ro transaction' do 
    client = InitialPhaseClient.new(stub_client)
    client.begin_snapshot
    client.next.must_be :kind_of?, SnapshotClient
  end
end
