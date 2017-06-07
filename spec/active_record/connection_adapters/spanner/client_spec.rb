require 'spec_helper'

describe ActiveRecord::ConnectionAdapters::Spanner::InitialPhaseClient do
  InitialPhaseClient = ActiveRecord::ConnectionAdapters::Spanner::InitialPhaseClient
  ReadPhaseClient = ActiveRecord::ConnectionAdapters::Spanner::ReadPhaseClient
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
