module ActiveRecord
  module ConnectionAdapters
    module Spanner
      # raised on unsupported operations in a certain transaction state.
      class TransactionStateError < ActiveRecordError
      end

      class InitialPhaseClient
        def initialize(client)
          @client = client
          @next = self
        end

        attr_reader :next

        delegate :insert, :update, :delete, :execute, :close, to: :client

        def begin_transaction
          @next = ReadWriteTransactionClient.begin(client)
        end

        def begin_snapshot(**options)
          @next = SnapshotClient.begin(client, **options)
        end

        %w[ commit rollback ].each do |name|
          class_eval(<<-"EOS", __FILE__, __LINE__+1)
            def #{name}
              raise TransactionStateError, 'not in a transaction'
            end
          EOS
        end

        private
        attr_reader :client
      end

      class ReadWriteTransactionClient
        def initialize(client, tx, tx_closer)
          @next = self
          @readable = true
          @client = client
          @tx = tx

          pending_commit = @tx.enum_for(:commit)
          @commit_op = ->{
            begin
              loop { pending_commit.next }
            ensure
              tx_closer.call
            end
          }
          @rollback_op = ->{
            begin
              tx.safe_rollback
            ensure
              tx_closer.call
            end
          }

          begin
            @mutations = pending_commit.next
          rescue
            tx_closer.call
            raise
          end
        end

        def self.begin(client)
          enum = client.enum_for(:transaction, deadline: 0)
          closer = ->{ loop { enum.next } }

          begin
            return new(client, enum.next, closer)
          rescue
            closer.call
            raise
          end
        end

        attr_reader :next

        delegate :close, to: :client
        #delegate :execute, to: :tx
        # delegate :insert, :update, :delete, to: :mutations

        %w[ insert update delete ].each do |name|
          class_eval(<<-"EOS", __FILE__, __LINE__+1)
            def #{name}(*args)
              mutations.#{name}(*args).tap do
                @readable = false
              end
            end
          EOS
        end

        def execute(*args)
          return tx.execute(*args) if @readable
          raise TransactionStateError, "cannot read after write within a transaction"
        end

        def commit
          @commit_op.call
          @next = InitialPhaseClient.new(@client)
        end

        def rollback
          @rollback_op.call
          @next = InitialPhaseClient.new(@client)
        end

        private
        attr_reader :mutations, :client, :tx  # :nodoc:
      end

      class SnapshotClient
        def initialize(client, snapshot)
          @client = client
          @snapshot = snapshot
          @next = self
        end

        attr_reader :next

        def self.begin(client, **options)
          enum = client.enum_for(:snapshot, **options)
          new(client, enum.next)
        end

        delegate :close, to: :client
        delegate :execute, to: :snapshot

        %w[ insert update delete ].each do |name|
          class_eval(<<-"EOS", __FILE__, __LINE__+1)
            def #{name}(*)
              raise TransactionStateError, "cannot write within a read-only transaction"
            end
          EOS
        end

        %w[ commit rollback ].each do |name|
          class_eval(<<-"EOS", __FILE__, __LINE__+1)
            def #{name}
              @next = InitialPhaseClient.new(@client)
              true
            end
          EOS
        end

        private
        attr_reader :client, :snapshot
      end
    end
  end
end
