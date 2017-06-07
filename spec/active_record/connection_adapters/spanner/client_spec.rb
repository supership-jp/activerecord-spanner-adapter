require 'spec_helper'

describe ActiveRecord::ConnectionAdapters::Spanner::InitialPhaseClient do
  InitialPhaseClient = ActiveRecord::ConnectionAdapters::Spanner::InitialPhaseClient
  ReadPhaseClient = ActiveRecord::ConnectionAdapters::Spanner::ReadPhaseClient
  WritePhaseClient = ActiveRecord::ConnectionAdapters::Spanner::WritePhaseClient
  ReadWriteTransactionClient = ActiveRecord::ConnectionAdapters::Spanner::ReadWriteTransactionClient
  SnapshotClient = ActiveRecord::ConnectionAdapters::Spanner::SnapshotClient

  let(:example_table) { 'foo' }
  let(:example_rows) {
    [
      {id: 1, name: 'a'},
      {id: 2, name: 'b'},
    ]
  }
  let(:raw_client) {
    double("client").tap do |client|
      %w[ insert update delete ].each do |method|
        allow(client).to receive(method).
          with(kind_of(String), kind_of(Array)).and_return(true)
      end
    end
  }

  subject { InitialPhaseClient.new(raw_client) }

  context 'when inserts rows' do
    it 'forwards insertion' do
      expect(raw_client).to receive(:insert).
        with(example_table, example_rows).and_return(true)

      subject.insert(example_table, example_rows)
    end

    it 'does not change state' do
      subject.insert(example_table, example_rows)
      expect(subject.next).to be(subject)
    end
  end

  context 'when updates rows' do
    it 'forwards update' do
      expect(raw_client).to receive(:update).
        with(example_table, example_rows).and_return(true)

      subject.update(example_table, example_rows)
    end

    it 'does not change state' do
      subject.update(example_table, example_rows)
      expect(subject.next).to be(subject)
    end
  end

  context 'when deletes rows' do
    let(:example_keyset) { [1, 2, 3] }
    it 'forwards deletion' do
      expect(raw_client).to receive(:delete).
        with(example_table, example_keyset).and_return(true)

      subject.delete(example_table, example_keyset)
    end

    it 'does not change state' do
      subject.delete(example_table, example_keyset)
      expect(subject.next).to be(subject)
    end
  end

  it 'does not support committing transactions' do
    expect { subject.commit }.to raise_error(ActiveRecord::ConnectionAdapters::Spanner::TransactionStateError)
  end

  it 'does not support rolling transactions back' do
    expect { subject.rollback }.to raise_error(ActiveRecord::ConnectionAdapters::Spanner::TransactionStateError)
  end

  context 'when begins a r/w transaction' do
    let(:next_client) { double(:next_client) }

    it 'begins ReadWriteTransactionClient' do 
      expect(ReadWriteTransactionClient).to receive(:begin).with(raw_client)
      subject.begin_transaction
    end

    it 'transits to read phase' do 
      allow(ReadWriteTransactionClient).to receive(:begin).with(raw_client).
        and_return(next_client)
      subject.begin_transaction
      expect(subject.next).to be(next_client)
    end
  end

  context 'when begins a snapshot transaction' do
    let(:next_client) { double(:next_client) }
    let(:options) { { timestamp: Time.now } }

    it 'begins SnapshotClient' do 
      expect(SnapshotClient).to receive(:begin).with(raw_client, **options)
      subject.begin_snapshot(**options)
    end

    it 'transits to snapshot phase' do 
      allow(SnapshotClient).to receive(:begin).with(raw_client, **options).
        and_return(next_client)
      subject.begin_snapshot(**options)
      expect(subject.next).to be(next_client)
    end
  end
end

shared_examples 'a r/w transaction client' do
  let(:example_table) { 'foo' }
  let(:example_rows) {
    [
      {id: 1, name: 'a'},
      {id: 2, name: 'b'},
    ]
  }

  describe '#commit' do
    let(:timestamp) { Time.now }

    it 'completes the transaction block' do
      expect(raw_client).to receive(:transaction)

      subject.commit
    end

    it 'returns commit timestamp' do
      allow(raw_client).to receive(:transaction).
        and_yield(transaction).and_return(timestamp)

      expect(subject.commit).to eq(timestamp)
    end
  end

  describe '#rollback' do
    it 'raises Rollback error' do
      expect(raw_client).to receive(:transaction).with(kind_of(Hash)) do |&blk|
        expect {
          blk.call(transaction)
        }.to raise_error(Google::Cloud::Spanner::Rollback)
      end

      subject.rollback
    end
  end

  context 'when inserts records' do
    before do
      allow(transaction).to receive(:insert)
    end

    it 'forwards to the transaction' do
      expect(transaction).to receive(:insert).with(example_table, example_rows)

      subject.insert(example_table, example_rows)
    end

    it 'transits to write phase' do
      subject.insert(example_table, example_rows)

      expect(subject.next).to be_a_kind_of(WritePhaseClient)
    end
  end

  context 'when updates records' do
    before do
      allow(transaction).to receive(:update)
    end

    it 'forwards to the transaction' do
      expect(transaction).to receive(:update).with(example_table, example_rows)

      subject.update(example_table, example_rows)
    end

    it 'transits to write phase' do
      subject.update(example_table, example_rows)

      expect(subject.next).to be_a_kind_of(WritePhaseClient)
    end
  end

  context 'when deletes records' do
    let(:example_keyset) { [1, 2, 3] }

    before do
      allow(transaction).to receive(:delete)
    end

    it 'forwards to the transaction' do
      expect(transaction).to receive(:delete).with(example_table, example_keyset)

      subject.delete(example_table, example_keyset)
    end

    it 'transits to write phase' do
      subject.delete(example_table, example_keyset)

      expect(subject.next).to be_a_kind_of(WritePhaseClient)
    end
  end

  it 'does not support #begin_transaction' do
    expect {
      subject.begin_transaction
    }.to raise_error(ActiveRecord::ConnectionAdapters::Spanner::TransactionStateError)
  end

  it 'does not support #begin_snapshot' do
    expect {
      subject.begin_snapshot(strong: true)
    }.to raise_error(ActiveRecord::ConnectionAdapters::Spanner::TransactionStateError)
  end
end

describe ActiveRecord::ConnectionAdapters::Spanner::ReadWriteTransactionClient, '.begin' do
  let(:transaction) { double(:transaction) }
  let(:raw_client) { double(:client) }

  before do
    allow(raw_client).to receive(:transaction).with(kind_of(Hash)).
      and_yield(transaction)
  end

    it 'begins a transaction block' do
      invoked, returned = false, false
      # NOTE: cannot use expect(raw_client) because the control does not
      # return from blk.call and the mock cannot record the method invocation.
      allow(raw_client).to receive(:transaction).with(kind_of(Hash)) do |&blk|
        invoked = true
        blk.call(transaction)
        returned = true
      end

      ReadWriteTransactionClient.begin(raw_client)
      expect(invoked).to be true
      expect(returned).not_to be true
    end

    it 'returns a ReadPhaseClient' do
      client = ReadWriteTransactionClient.begin(raw_client)
      expect(client).to be_a_kind_of(ReadPhaseClient)
    end
end

describe ActiveRecord::ConnectionAdapters::Spanner::ReadPhaseClient do
  let(:transaction) { double(:transaction) }
  let(:raw_client) { double(:client) }

  before do
    allow(raw_client).to receive(:transaction).with(kind_of(Hash)).
      and_yield(transaction)
  end

  it_behaves_like 'a r/w transaction client' do
    subject {
      ReadWriteTransactionClient.begin(raw_client)
    }
  end

  context 'when executes a query' do
    let(:example_query) { <<-QUERY }
      SELECT
        a, COUNT(b), MAX(c)
      FROM
        example_table
      WHERE
        user_id = @user AND @date BETWEEN @from AND @to
      GROUP BY
        a
    QUERY
    let(:example_params) {
      {
        user: 1,
        from: Date.today - 1,
        to: Date.today,
      }
    }
    let(:dummy_result) { double(:dummy_result) }

    before do
      allow(transaction).to receive(:execute).and_return(dummy_result)
    end

    subject {
      ReadPhaseClient.new(
        raw_client,
        transaction,
        double(:dummy_commit_op),
        double(:dummy_rollback_op),
      )
    }


    it 'forwards to the tx' do
      expect(transaction).to receive(:execute).with(example_query, example_params)

      subject.execute(example_query, example_params)
    end

    it 'returns the result from the tx' do
      result = subject.execute(example_query, example_params)
      expect(result).to be(dummy_result)
    end

    it 'does not change the phase' do
      subject.execute(example_query, example_params)
      expect(subject.next).to be(subject)
    end
  end
end

describe ActiveRecord::ConnectionAdapters::Spanner::WritePhaseClient do
  let(:transaction) { double(:transaction) }
  let(:raw_client) { double(:client) }

  before do
    allow(raw_client).to receive(:transaction).with(kind_of(Hash)).
      and_yield(transaction)
  end

  subject {
    read = ReadWriteTransactionClient.begin(raw_client)
    # TODO: better way to keep this description decoupled from
    # both of the behavior of ReadPhaseClient and its internal implementation.
    read.send(:transit_to_write_phase)
    raise TypeError unless read.next.kind_of?(WritePhaseClient)
    read.next
  }

  it_behaves_like 'a r/w transaction client'

  context 'when executes a query' do
    let(:example_query) { "SELECT a FROM example_table" }
    let(:example_params) { Hash.new }

    before do
      allow(transaction).to receive(:execute)
    end

    it 'raises TransactionStateError' do
      expect {
        subject.execute(example_query, example_params)
      }.to raise_error(ActiveRecord::ConnectionAdapters::Spanner::TransactionStateError)
    end
  end
end

describe ActiveRecord::ConnectionAdapters::Spanner::SnapshotClient do
  let(:snapshot) { double(:snapshot) }
  let(:raw_client) { double(:client) }
  let(:example_options) {
    {
      read_timetamp: Time.now
    }
  }

  before do
    allow(raw_client).to receive(:snapshot).with(kind_of(Hash)).
      and_yield(snapshot)
  end

  subject {
    SnapshotClient.begin(raw_client, **example_options)
  }

  describe '.begin' do
    it 'begins a snapshot block' do
      invoked, returned = false, false
      # NOTE: cannot use expect(raw_client) because the control does not
      # return from blk.call and the mock cannot record the method invocation.
      allow(raw_client).to receive(:snapshot).with(**example_options) do |&blk|
        invoked = true
        blk.call(snapshot)
        returned = true
      end

      subject

      expect(invoked).to be true
      expect(returned).not_to be true
    end
  end

  describe '#commit' do
    it 'completes the snapshot block' do
      expect(raw_client).to receive(:snapshot).with(**example_options)

      subject.commit
    end

    it 'transit to the initial phase' do
      subject.commit

      expect(subject.next).to be_a_kind_of(InitialPhaseClient)
    end
  end

  describe '#rollback' do
    it 'completes the snapshot block' do
      expect(raw_client).to receive(:snapshot).with(**example_options)

      subject.commit
    end

    it 'transit to the initial phase' do
      subject.commit

      expect(subject.next).to be_a_kind_of(InitialPhaseClient)
    end
  end

  let(:example_table) { 'foo' }
  let(:example_rows) {
    [
      {id: 1, name: 'a'},
      {id: 2, name: 'b'},
    ]
  }

  it 'does not support #insert' do
    expect {
      subject.insert(example_table, example_rows)
    }.to raise_error(ActiveRecord::ConnectionAdapters::Spanner::TransactionStateError)
  end

  it 'does not support #update' do
    expect {
      subject.update(example_table, example_rows)
    }.to raise_error(ActiveRecord::ConnectionAdapters::Spanner::TransactionStateError)
  end

  it 'does not support #delete' do
    example_keyset = [1, 2, 3]
    expect {
      subject.delete(example_table, example_keyset)
    }.to raise_error(ActiveRecord::ConnectionAdapters::Spanner::TransactionStateError)
  end

  it 'does not support #begin_transaction' do
    expect {
      subject.begin_transaction
    }.to raise_error(ActiveRecord::ConnectionAdapters::Spanner::TransactionStateError)
  end

  it 'does not support #begin_snapshot' do
    expect {
      subject.begin_snapshot(strong: true)
    }.to raise_error(ActiveRecord::ConnectionAdapters::Spanner::TransactionStateError)
  end
end
